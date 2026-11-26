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

//! Conversions between `jj_lib::backend` domain types and `cc_common::backend` protobufs.
//!
//! # Cross-Process and RPC Boundaries
//! `commit_from_proto` intentionally leaves `Commit.secure_sig` unset (`None`) so unverified
//! wire `SecureSig.data` bytes can never enter `jj_lib::backend::Commit`; callers that expose
//! commit signatures to `jj_lib` must reconstruct and verify the signed payload against the
//! commit's canonical bytes or digest-verified `raw_git_commit`.

use jj_lib::backend::*;
use jj_lib::object_id::ObjectId;
use jj_lib::repo_path::RepoPathComponentBuf;

use crate::backend as pb;

pub fn signature_to_proto(sig: &Signature) -> pb::Signature {
    pb::Signature {
        name: sig.name.clone(),
        email: sig.email.clone(),
        timestamp: Some(pb::Timestamp {
            millis_since_epoch: sig.timestamp.timestamp.0,
            tz_offset: sig.timestamp.tz_offset,
        }),
    }
}

pub fn signature_from_proto(proto: pb::Signature) -> Signature {
    let ts = proto.timestamp.unwrap_or_default();
    Signature {
        name: proto.name,
        email: proto.email,
        timestamp: Timestamp {
            timestamp: MillisSinceEpoch(ts.millis_since_epoch),
            tz_offset: ts.tz_offset,
        },
    }
}

pub fn commit_to_proto(commit: &Commit) -> pb::Commit {
    pb::Commit {
        commit_id: vec![],
        change_id: commit.change_id.to_bytes().to_vec(),
        parent_commit_ids: commit
            .parents
            .iter()
            .map(|id| id.to_bytes().to_vec())
            .collect(),
        root_tree_id: commit
            .root_tree
            .iter()
            .map(|id| id.to_bytes().to_vec())
            .collect(),
        description: commit.description.clone(),
        author: Some(signature_to_proto(&commit.author)),
        committer: Some(signature_to_proto(&commit.committer)),
        predecessors: commit
            .predecessors
            .iter()
            .map(|id| id.to_bytes().to_vec())
            .collect(),
        conflict_labels: jj_lib::conflict_labels::ConflictLabels::from_merge(
            commit.conflict_labels.clone(),
        )
        .as_slice()
        .to_owned(),
        secure_sig: commit.secure_sig.as_ref().map(|s| pb::SecureSig {
            data: s.data.clone(),
            sig: s.sig.clone(),
        }),
        raw_git_commit: Vec::new(),
        external_ids: Default::default(),
    }
}

/// Converts a protobuf `Commit` into a `jj_lib::backend::Commit` without trusting unverified
/// `SecureSig.data` bytes from the wire (`secure_sig` is always returned as `None`).
pub fn commit_from_proto(proto_commit: pb::Commit) -> Commit {
    let author = signature_from_proto(proto_commit.author.unwrap_or_default());
    let committer = signature_from_proto(proto_commit.committer.unwrap_or_default());

    let merge_builder: jj_lib::merge::MergeBuilder<_> = proto_commit
        .root_tree_id
        .into_iter()
        .map(|b| TreeId::from_bytes(&b))
        .collect();

    let root_tree = merge_builder.build();
    let conflict_labels =
        jj_lib::conflict_labels::ConflictLabels::from_vec(proto_commit.conflict_labels)
            .into_merge();

    Commit {
        parents: proto_commit
            .parent_commit_ids
            .iter()
            .map(|b| CommitId::from_bytes(b))
            .collect(),
        predecessors: proto_commit
            .predecessors
            .iter()
            .map(|b| CommitId::from_bytes(b))
            .collect(),
        root_tree,
        change_id: ChangeId::from_bytes(&proto_commit.change_id),
        description: proto_commit.description,
        author,
        committer,
        conflict_labels,
        secure_sig: None,
    }
}

pub fn tree_entry_to_proto(entry: &jj_lib::backend::TreeEntry) -> BackendResult<pb::TreeEntry> {
    let value = match entry.value() {
        TreeValue::File {
            id,
            executable,
            copy_id,
        } => pb::TreeValue {
            value: Some(pb::tree_value::Value::File(pb::File {
                id: id.to_bytes().to_vec(),
                executable: *executable,
                copy_id: copy_id.to_bytes().to_vec(),
                size_bytes: None,
            })),
        },
        TreeValue::Symlink(id) => pb::TreeValue {
            value: Some(pb::tree_value::Value::SymlinkId(id.to_bytes().to_vec())),
        },
        TreeValue::Tree(id) => pb::TreeValue {
            value: Some(pb::tree_value::Value::TreeId(id.to_bytes().to_vec())),
        },
        TreeValue::GitSubmodule(id) => pb::TreeValue {
            value: Some(pb::tree_value::Value::GitSubmoduleId(
                id.to_bytes().to_vec(),
            )),
        },
    };

    Ok(pb::TreeEntry {
        name: entry.name().as_internal_str().to_string(),
        value: Some(value),
    })
}

pub fn tree_entry_from_proto(
    proto_entry: pb::TreeEntry,
) -> Result<(RepoPathComponentBuf, TreeValue), Box<dyn std::error::Error + Send + Sync>> {
    if proto_entry.name == "." || proto_entry.name == ".." || proto_entry.name.contains('\0') {
        return Err(format!("invalid tree entry name: {:?}", proto_entry.name).into());
    }
    let component = RepoPathComponentBuf::new(proto_entry.name)?;
    let proto_val = proto_entry
        .value
        .ok_or("tree entry should have contained a TreeValue")?;
    let val = match proto_val
        .value
        .ok_or("TreeValue should have contained an inner value")?
    {
        pb::tree_value::Value::File(f) => {
            if f.id.len() != crate::FILE_ID_LENGTH {
                return Err(format!(
                    "file id must be {} bytes (got {} bytes)",
                    crate::FILE_ID_LENGTH,
                    f.id.len()
                )
                .into());
            }
            if !f.copy_id.is_empty() {
                return Err(format!(
                    "non-empty file copy_id ({} bytes) is not supported",
                    f.copy_id.len()
                )
                .into());
            }
            TreeValue::File {
                id: FileId::from_bytes(&f.id),
                executable: f.executable,
                copy_id: CopyId::placeholder(),
            }
        }
        pb::tree_value::Value::SymlinkId(id) => {
            if id.len() != crate::SYMLINK_ID_LENGTH {
                return Err(format!(
                    "symlink id must be {} bytes (got {} bytes)",
                    crate::SYMLINK_ID_LENGTH,
                    id.len()
                )
                .into());
            }
            TreeValue::Symlink(SymlinkId::from_bytes(&id))
        }
        pb::tree_value::Value::TreeId(id) => {
            if id.len() != crate::TREE_ID_LENGTH {
                return Err(format!(
                    "tree id must be {} bytes (got {} bytes)",
                    crate::TREE_ID_LENGTH,
                    id.len()
                )
                .into());
            }
            TreeValue::Tree(TreeId::from_bytes(&id))
        }
        pb::tree_value::Value::GitSubmoduleId(id) => {
            if id.len() != crate::COMMIT_ID_LENGTH {
                return Err(format!(
                    "git submodule commit id must be {} bytes (got {} bytes)",
                    crate::COMMIT_ID_LENGTH,
                    id.len()
                )
                .into());
            }
            TreeValue::GitSubmodule(CommitId::from_bytes(&id))
        }
        pb::tree_value::Value::ConflictId(_) => {
            return Err("tree entry should not have contained a ConflictId".into());
        }
    };
    Ok((component, val))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn commit_from_proto_strips_unverified_secure_sig() {
        let proto = pb::Commit {
            commit_id: vec![0x11; crate::COMMIT_ID_LENGTH],
            change_id: vec![0x22; crate::CHANGE_ID_LENGTH],
            parent_commit_ids: vec![crate::ROOT_COMMIT_ID_BYTES.to_vec()],
            root_tree_id: vec![crate::EMPTY_TREE_ID_BYTES.to_vec()],
            description: "signed".to_string(),
            author: None,
            committer: None,
            predecessors: Vec::new(),
            conflict_labels: Vec::new(),
            secure_sig: Some(pb::SecureSig {
                data: b"forged payload".to_vec(),
                sig: b"forged sig".to_vec(),
            }),
            raw_git_commit: Vec::new(),
            external_ids: Default::default(),
        };
        let commit = commit_from_proto(proto);
        assert!(commit.secure_sig.is_none());
    }

    #[test]
    fn commit_to_proto_normalizes_unlabeled_conflict_labels_to_empty() {
        let base_commit = Commit {
            parents: vec![CommitId::from_bytes(&crate::ROOT_COMMIT_ID_BYTES)],
            predecessors: Vec::new(),
            root_tree: jj_lib::merge::Merge::resolved(TreeId::from_bytes(
                &crate::EMPTY_TREE_ID_BYTES,
            )),
            conflict_labels: jj_lib::merge::Merge::resolved(String::new()),
            change_id: ChangeId::from_bytes(&[0x33; crate::CHANGE_ID_LENGTH]),
            description: "resolved".to_string(),
            author: signature_from_proto(pb::Signature::default()),
            committer: signature_from_proto(pb::Signature::default()),
            secure_sig: None,
        };
        let proto = commit_to_proto(&base_commit);
        assert!(
            proto.conflict_labels.is_empty(),
            "resolved commits must serialize unlabeled conflict_labels as [], got {:?}",
            proto.conflict_labels
        );

        let mut all_empty_conflict = base_commit.clone();
        all_empty_conflict.root_tree = jj_lib::merge::Merge::from_vec(vec![
            TreeId::from_bytes(&[0x11; crate::TREE_ID_LENGTH]),
            TreeId::from_bytes(&[0x22; crate::TREE_ID_LENGTH]),
            TreeId::from_bytes(&[0x33; crate::TREE_ID_LENGTH]),
        ]);
        all_empty_conflict.conflict_labels =
            jj_lib::merge::Merge::from_vec(vec![String::new(), String::new(), String::new()]);
        assert!(
            commit_to_proto(&all_empty_conflict)
                .conflict_labels
                .is_empty()
        );

        let mut partial_labeled = all_empty_conflict;
        partial_labeled.conflict_labels = jj_lib::merge::Merge::from_vec(vec![
            "left".to_string(),
            String::new(),
            "right".to_string(),
        ]);
        assert_eq!(
            commit_to_proto(&partial_labeled).conflict_labels,
            vec!["left", "", "right"]
        );
    }

    #[test]
    fn tree_entry_roundtrips_git_submodule_and_placeholder_copy_id() {
        let file_id = FileId::from_bytes(&[0xaa; crate::FILE_ID_LENGTH]);
        let sub_id = CommitId::from_bytes(&[0xbb; crate::COMMIT_ID_LENGTH]);
        let tree = Tree::from_sorted_entries(vec![
            (
                RepoPathComponentBuf::new("file.txt".to_string()).unwrap(),
                TreeValue::File {
                    id: file_id,
                    executable: true,
                    copy_id: CopyId::placeholder(),
                },
            ),
            (
                RepoPathComponentBuf::new("submod".to_string()).unwrap(),
                TreeValue::GitSubmodule(sub_id),
            ),
        ]);

        for entry in tree.entries() {
            let proto = tree_entry_to_proto(&entry).unwrap();
            let (name, value) = tree_entry_from_proto(proto).unwrap();
            assert_eq!(name.as_internal_str(), entry.name().as_internal_str());
            assert_eq!(value, *entry.value());
        }
    }

    #[test]
    fn tree_entry_from_proto_rejects_bad_lengths_and_non_empty_copy_id() {
        let bad_file = pb::TreeEntry {
            name: "a".to_string(),
            value: Some(pb::TreeValue {
                value: Some(pb::tree_value::Value::File(pb::File {
                    id: vec![1, 2, 3],
                    executable: false,
                    copy_id: Vec::new(),
                    size_bytes: None,
                })),
            }),
        };
        assert!(tree_entry_from_proto(bad_file).is_err());

        let non_empty_copy_id = pb::TreeEntry {
            name: "b".to_string(),
            value: Some(pb::TreeValue {
                value: Some(pb::tree_value::Value::File(pb::File {
                    id: vec![0xaa; crate::FILE_ID_LENGTH],
                    executable: false,
                    copy_id: vec![0x01; 20],
                    size_bytes: None,
                })),
            }),
        };
        assert!(tree_entry_from_proto(non_empty_copy_id).is_err());

        let bad_submod = pb::TreeEntry {
            name: "s".to_string(),
            value: Some(pb::TreeValue {
                value: Some(pb::tree_value::Value::GitSubmoduleId(vec![0xbb; 19])),
            }),
        };
        assert!(tree_entry_from_proto(bad_submod).is_err());
    }

    #[test]
    fn tree_entry_from_proto_rejects_dot_dotdot_and_nul_names() {
        for bad_name in [".", "..", "foo\0bar", "\0"] {
            let entry = pb::TreeEntry {
                name: bad_name.to_string(),
                value: Some(pb::TreeValue {
                    value: Some(pb::tree_value::Value::File(pb::File {
                        id: vec![0xaa; crate::FILE_ID_LENGTH],
                        executable: false,
                        copy_id: Vec::new(),
                        size_bytes: None,
                    })),
                }),
            };
            assert!(
                tree_entry_from_proto(entry).is_err(),
                "tree entry name {bad_name:?} must be rejected"
            );
        }
    }
}
