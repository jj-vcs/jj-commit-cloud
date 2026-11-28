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

//! Commit protobuf encoding, canonical Git commit serialization, and signature
//! verification for [`super::CommitCloudBackend`].
//!
//! # Authority and Crash-Safety Ordering
//! `apply_commit_signing` formats the canonical unsigned Git commit payload before
//! invoking `jj_lib`'s signing callback so client and server derive identical Git
//! commit digests.
//!
//! # Concurrency and Merge Invariants
//! All helpers are pure functions over protobuf and `jj_lib::backend::Commit`
//! values. `verified_commit_from_proto` checks structural invariants, external ID
//! constraints, canonical SHA-1 digests, and `gpgsig` header parity before exposing
//! a commit or its `SecureSig` to `jj_lib`.

use jj_lib::backend::*;
use jj_lib::object_id::ObjectId;

const EMPTY_STRING_PLACEHOLDER: &str = "JJ_EMPTY_STRING";

fn signature_to_proto(sig: &Signature) -> cc_common::backend::Signature {
    cc_common::backend::Signature {
        name: sig.name.clone(),
        email: sig.email.clone(),
        timestamp: Some(cc_common::backend::Timestamp {
            millis_since_epoch: sig.timestamp.timestamp.0,
            tz_offset: sig.timestamp.tz_offset,
        }),
    }
}

fn signature_from_proto(proto: cc_common::backend::Signature) -> Signature {
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

pub fn commit_to_proto(commit: &Commit) -> cc_common::backend::Commit {
    cc_common::backend::Commit {
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
        secure_sig: commit
            .secure_sig
            .as_ref()
            .map(|s| cc_common::backend::SecureSig {
                data: s.data.clone(),
                sig: s.sig.clone(),
            }),
        raw_git_commit: Vec::new(),
        external_ids: Default::default(),
    }
}

pub(super) fn sha1_object_id(bytes: &[u8], field: &str) -> BackendResult<gix::hash::ObjectId> {
    if bytes.len() != cc_common::COMMIT_ID_LENGTH {
        return Err(BackendError::Other(
            format!(
                "{field} must be {} bytes (got {} bytes)",
                cc_common::COMMIT_ID_LENGTH,
                bytes.len()
            )
            .into(),
        ));
    }
    gix::hash::ObjectId::try_from(bytes)
        .map_err(|e| BackendError::Other(format!("invalid {field}: {e}").into()))
}

pub(super) fn canonical_git_commit_bytes(
    proto: &cc_common::backend::Commit,
    sig_override: Option<&[u8]>,
) -> BackendResult<Vec<u8>> {
    const REVERSE_HEX_CHARS: &[u8; 16] = b"zyxwvutsrqponmlk";
    const FORWARD_HEX_CHARS: &[u8; 16] = b"0123456789abcdef";

    use gix::objs::WriteTo as _;

    let tree_ids = proto
        .root_tree_id
        .iter()
        .map(|id| sha1_object_id(id.as_slice(), "root_tree_id"))
        .collect::<BackendResult<Vec<_>>>()?;
    let tree_id = *tree_ids
        .first()
        .ok_or_else(|| BackendError::Other("commit must have at least one root tree".into()))?;

    let mut parents = Vec::with_capacity(proto.parent_commit_ids.len());
    for id in &proto.parent_commit_ids {
        if id.as_slice() == cc_common::ROOT_COMMIT_ID_BYTES {
            continue;
        }
        parents.push(sha1_object_id(id.as_slice(), "parent_commit_ids")?);
    }

    let mut extra_headers = Vec::new();
    if proto.root_tree_id.len() > 1
        && proto.conflict_labels.len() > 1
        && proto.conflict_labels.iter().any(|l| !l.is_empty())
    {
        let mut joined = proto.conflict_labels.join("\n");
        joined.push('\n');
        extra_headers.push(("jj:conflict-labels".into(), joined.into()));
    }
    if proto.root_tree_id.len() > 1 {
        let trees_hex: Vec<String> = proto
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
    if !proto.change_id.is_empty() {
        let rev_bytes: Vec<u8> = proto
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
    }
    if let Some(sig) = sig_override
        && !sig.is_empty()
    {
        extra_headers.push(("gpgsig".into(), sig.to_vec().into()));
    }

    let sig_to_git =
        |s: Option<&cc_common::backend::Signature>| -> BackendResult<gix::actor::Signature> {
            let (name, email) = match s {
                Some(sig) => (
                    if sig.name.is_empty() {
                        EMPTY_STRING_PLACEHOLDER
                    } else {
                        sig.name.as_str()
                    },
                    if sig.email.is_empty() {
                        EMPTY_STRING_PLACEHOLDER
                    } else {
                        sig.email.as_str()
                    },
                ),
                None => (EMPTY_STRING_PLACEHOLDER, EMPTY_STRING_PLACEHOLDER),
            };
            let (millis, tz_offset) = s
                .and_then(|sig| sig.timestamp.as_ref())
                .map_or((0, 0), |t| (t.millis_since_epoch, t.tz_offset));
            let tz_secs = tz_offset.checked_mul(60).ok_or_else(|| {
                BackendError::Other(
                    format!("signature timezone offset {tz_offset} overflows seconds").into(),
                )
            })?;
            if tz_secs.unsigned_abs() / 3_600 > 99 {
                return Err(BackendError::Other(
                    format!(
                        "signature timezone offset {tz_offset} minutes exceeds Git +-9959 limit"
                    )
                    .into(),
                ));
            }
            Ok(gix::actor::Signature {
                name: name.into(),
                email: email.into(),
                time: gix::date::Time::new(millis.div_euclid(1000), tz_secs),
            })
        };

    let gix_commit = gix::objs::Commit {
        tree: tree_id,
        parents: parents.into(),
        author: sig_to_git(proto.author.as_ref())?,
        committer: sig_to_git(proto.committer.as_ref())?,
        encoding: None,
        extra_headers,
        message: proto.description.as_bytes().into(),
    };
    let mut bytes = Vec::new();
    gix_commit
        .write_to(&mut bytes)
        .map_err(|e| BackendError::Other(Box::new(e)))?;
    Ok(bytes)
}

fn raw_signature_matches(
    sig: Option<&cc_common::backend::Signature>,
    raw: gix::actor::SignatureRef<'_>,
) -> bool {
    let text_matches = |structured: &str, raw: &gix::bstr::BStr| {
        structured == String::from_utf8_lossy(raw)
            || (structured.is_empty() && raw == EMPTY_STRING_PLACEHOLDER)
    };
    let (name, email) = sig.map_or(("", ""), |s| (s.name.as_str(), s.email.as_str()));
    let (millis, tz_offset) = sig
        .and_then(|s| s.timestamp.as_ref())
        .map_or((0, 0), |t| (t.millis_since_epoch, t.tz_offset));
    let Ok(raw_time) = raw.time() else {
        return false;
    };
    text_matches(name, raw.name)
        && text_matches(email, raw.email)
        && raw_time.seconds.checked_mul(1000) == Some(millis)
        && raw_time.offset.div_euclid(60) == tz_offset
}

fn verify_raw_git_commit_matches_proto(
    proto_commit: &cc_common::backend::Commit,
    parsed: &gix::objs::CommitRef<'_>,
    expected_id: &CommitId,
) -> BackendResult<()> {
    if parsed.extra_headers().find("jj:trees").is_none() {
        let expected_first_tree = proto_commit
            .root_tree_id
            .first()
            .map(Vec::as_slice)
            .unwrap_or(&cc_common::EMPTY_TREE_ID_BYTES);
        if parsed.tree().as_bytes() != expected_first_tree {
            return Err(BackendError::Other(
                "ReadCommit raw_git_commit tree header does not match commit.root_tree_id".into(),
            ));
        }
    }

    let structured_parents: Vec<&[u8]> = proto_commit
        .parent_commit_ids
        .iter()
        .map(Vec::as_slice)
        .filter(|id| *id != cc_common::ROOT_COMMIT_ID_BYTES)
        .collect();
    let raw_parents: Vec<gix::hash::ObjectId> = parsed.parents().collect();
    if raw_parents.len() != structured_parents.len()
        || raw_parents
            .iter()
            .zip(structured_parents.iter())
            .any(|(rp, sp)| rp.as_bytes() != *sp)
    {
        return Err(BackendError::Other(
            "ReadCommit raw_git_commit parent headers do not match commit.parent_commit_ids".into(),
        ));
    }

    if proto_commit.description != String::from_utf8_lossy(parsed.message) {
        return Err(BackendError::Other(
            "ReadCommit raw_git_commit message does not match commit.description".into(),
        ));
    }
    let raw_author = parsed
        .author()
        .map_err(|e| BackendError::Other(format!("invalid raw_git_commit author: {e}").into()))?;
    if !raw_signature_matches(proto_commit.author.as_ref(), raw_author) {
        return Err(BackendError::Other(
            "ReadCommit raw_git_commit author does not match commit.author".into(),
        ));
    }
    let raw_committer = parsed.committer().map_err(|e| {
        BackendError::Other(format!("invalid raw_git_commit committer: {e}").into())
    })?;
    if !raw_signature_matches(proto_commit.committer.as_ref(), raw_committer) {
        return Err(BackendError::Other(
            "ReadCommit raw_git_commit committer does not match commit.committer".into(),
        ));
    }

    let expected_change_id = parsed
        .extra_headers()
        .find("change-id")
        .and_then(jj_lib::hex_util::decode_reverse_hex)
        .filter(|id| id.len() == cc_common::CHANGE_ID_LENGTH)
        .unwrap_or_else(|| {
            jj_lib::git_backend::synthetic_change_id_from_git_commit_id(expected_id)
                .as_bytes()
                .to_vec()
        });
    if proto_commit.change_id != expected_change_id {
        return Err(BackendError::Other(
            "ReadCommit raw_git_commit change-id does not match commit.change_id".into(),
        ));
    }

    match parsed.extra_headers().find("jj:trees") {
        Some(value) => {
            let tree_ids = value
                .split(|b| *b == b' ')
                .map(|hex| {
                    jj_lib::hex_util::decode_hex(hex)
                        .filter(|id| id.len() == cc_common::TREE_ID_LENGTH)
                })
                .collect::<Option<Vec<_>>>()
                .filter(|ids| ids.len() > 1 && ids.len() % 2 == 1)
                .ok_or_else(|| {
                    BackendError::Other("ReadCommit raw_git_commit has invalid jj:trees".into())
                })?;
            if tree_ids != proto_commit.root_tree_id {
                return Err(BackendError::Other(
                    "ReadCommit raw_git_commit jj:trees does not match commit.root_tree_id".into(),
                ));
            }
        }
        None if proto_commit.root_tree_id.len() != 1 => {
            return Err(BackendError::Other(
                "ReadCommit commit.root_tree_id lists conflict terms absent from raw_git_commit"
                    .into(),
            ));
        }
        None => {}
    }

    match parsed.extra_headers().find("jj:conflict-labels") {
        Some(value) => {
            let mut labels: Vec<&str> = std::str::from_utf8(value)
                .map_err(|_| {
                    BackendError::Other(
                        "ReadCommit raw_git_commit jj:conflict-labels is not UTF-8".into(),
                    )
                })?
                .split_terminator('\n')
                .collect();
            if labels.iter().all(|label| label.is_empty()) {
                let valid_empty_len = labels.is_empty()
                    || labels.len() == 1
                    || labels.len() == proto_commit.root_tree_id.len();
                if !valid_empty_len {
                    return Err(BackendError::Other(
                        format!(
                            "ReadCommit raw_git_commit jj:conflict-labels has invalid length ({} labels for {} trees)",
                            labels.len(),
                            proto_commit.root_tree_id.len()
                        )
                        .into(),
                    ));
                }
                labels.clear();
            }
            if labels != proto_commit.conflict_labels {
                return Err(BackendError::Other(
                    "ReadCommit raw_git_commit jj:conflict-labels does not match commit.conflict_labels"
                        .into(),
                ));
            }
        }
        None if proto_commit.conflict_labels.iter().any(|l| !l.is_empty()) => {
            return Err(BackendError::Other(
                "ReadCommit commit.conflict_labels lists labels absent from raw_git_commit".into(),
            ));
        }
        None => {}
    }

    Ok(())
}

/// Formats the canonical unsigned Git commit payload matching `server::hash_utils` and
/// invokes `sign_with` when cryptographic commit signing is requested.
pub fn apply_commit_signing(
    commit: &mut Commit,
    sign_with: Option<&mut SigningFn>,
) -> BackendResult<()> {
    let Some(sign_fn) = sign_with else {
        return Ok(());
    };
    if commit.secure_sig.is_some() {
        return Err(BackendError::Other(
            "write_commit called with sign_with on a commit that already has secure_sig".into(),
        ));
    }

    let proto = commit_to_proto(commit);
    let unsigned_data = canonical_git_commit_bytes(&proto, None)?;
    let sig = sign_fn(&unsigned_data).map_err(|e| BackendError::Other(Box::new(e)))?;
    if sig.is_empty() {
        return Err(BackendError::Other(
            "commit signing callback returned empty signature bytes".into(),
        ));
    }
    commit.secure_sig = Some(SecureSig {
        data: unsigned_data,
        sig,
    });
    Ok(())
}

fn validate_external_ids(
    external_ids: &std::collections::HashMap<String, Vec<u8>>,
) -> BackendResult<()> {
    const MAX_ACTOR_EXTERNAL_ID_LEN: usize = 256;

    for (key, value) in external_ids {
        if key.is_empty()
            || key.len() > 128
            || key == "."
            || key == ".."
            || !key
                .bytes()
                .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'-' | b'_' | b'.'))
        {
            return Err(BackendError::Other(
                format!("ReadCommit invalid external_ids key {key:?}").into(),
            ));
        }
        match key.as_str() {
            "git-sha1" => {
                if value.len() != 20 {
                    return Err(BackendError::Other(
                        format!(
                            "ReadCommit external_ids['git-sha1'] must be 20 bytes (got {} bytes)",
                            value.len()
                        )
                        .into(),
                    ));
                }
            }
            "git-sha256" => {
                if value.len() != 32 {
                    return Err(BackendError::Other(
                        format!(
                            "ReadCommit external_ids['git-sha256'] must be 32 bytes (got {} bytes)",
                            value.len()
                        )
                        .into(),
                    ));
                }
            }
            "actor-sub" | "actor-uid" => {
                if value.is_empty() || value.len() > MAX_ACTOR_EXTERNAL_ID_LEN {
                    return Err(BackendError::Other(
                        format!(
                            "ReadCommit external_ids[{key:?}] must be 1..={MAX_ACTOR_EXTERNAL_ID_LEN} bytes (got {} bytes)",
                            value.len()
                        )
                        .into(),
                    ));
                }
                let text = std::str::from_utf8(value).map_err(|_| {
                    BackendError::Other(
                        format!("ReadCommit external_ids[{key:?}] must be valid UTF-8").into(),
                    )
                })?;
                if text.trim().is_empty() || text.bytes().any(|b| b.is_ascii_control()) {
                    return Err(BackendError::Other(
                        format!(
                            "ReadCommit external_ids[{key:?}] must not be blank or contain ASCII control characters"
                        )
                        .into(),
                    ));
                }
            }
            _ => {
                if value.is_empty() || value.len() > 64 {
                    return Err(BackendError::Other(
                        format!(
                            "ReadCommit external_ids[{key:?}] must be 1..=64 bytes (got {} bytes)",
                            value.len()
                        )
                        .into(),
                    ));
                }
            }
        }
    }
    Ok(())
}

fn validate_commit_proto_structure(
    proto_commit: &mut cc_common::backend::Commit,
) -> BackendResult<()> {
    if proto_commit.change_id.len() != cc_common::CHANGE_ID_LENGTH {
        return Err(BackendError::Other(
            format!(
                "ReadCommit change_id must be {} bytes (got {} bytes)",
                cc_common::CHANGE_ID_LENGTH,
                proto_commit.change_id.len()
            )
            .into(),
        ));
    }
    let num_trees = proto_commit.root_tree_id.len();
    if num_trees == 0 || num_trees.is_multiple_of(2) {
        return Err(BackendError::Other(
            format!("ReadCommit root_tree_id must have odd non-zero length (got {num_trees})")
                .into(),
        ));
    }
    for tid in &proto_commit.root_tree_id {
        if tid.len() != cc_common::TREE_ID_LENGTH {
            return Err(BackendError::Other(
                format!(
                    "ReadCommit root_tree_id entry must be {} bytes (got {} bytes)",
                    cc_common::TREE_ID_LENGTH,
                    tid.len()
                )
                .into(),
            ));
        }
    }
    let labels = &proto_commit.conflict_labels;
    let has_non_empty_label = labels.iter().any(|l| !l.is_empty());
    let unlabeled = labels.is_empty()
        || (labels.len() == 1 && labels[0].is_empty())
        || (labels.len() == num_trees && !has_non_empty_label);
    if !unlabeled && (num_trees == 1 || labels.len() != num_trees || !has_non_empty_label) {
        return Err(BackendError::Other(
            format!(
                "ReadCommit conflict_labels must be empty or match root_tree_id length (got {} labels for {num_trees} trees)",
                labels.len()
            )
            .into(),
        ));
    }
    if labels.iter().any(|l| l.contains('\n')) {
        return Err(BackendError::Other(
            "ReadCommit conflict_labels must not contain newlines".into(),
        ));
    }
    if unlabeled {
        proto_commit.conflict_labels.clear();
    }
    for pid in &proto_commit.parent_commit_ids {
        if pid.len() != cc_common::COMMIT_ID_LENGTH {
            return Err(BackendError::Other(
                format!(
                    "ReadCommit parent_commit_id must be {} bytes (got {} bytes)",
                    cc_common::COMMIT_ID_LENGTH,
                    pid.len()
                )
                .into(),
            ));
        }
    }
    for pred in &proto_commit.predecessors {
        if pred.len() != cc_common::COMMIT_ID_LENGTH {
            return Err(BackendError::Other(
                format!(
                    "ReadCommit predecessor must be {} bytes (got {} bytes)",
                    cc_common::COMMIT_ID_LENGTH,
                    pred.len()
                )
                .into(),
            ));
        }
    }
    Ok(())
}

pub(super) fn verified_commit_from_proto(
    expected_id: &CommitId,
    mut proto_commit: cc_common::backend::Commit,
) -> BackendResult<Commit> {
    if !proto_commit.commit_id.is_empty()
        && proto_commit.commit_id.as_slice() != expected_id.as_bytes()
    {
        return Err(BackendError::Other(
            format!(
                "ReadCommit response commit_id {} did not match requested {}",
                jj_lib::hex_util::encode_hex(&proto_commit.commit_id),
                expected_id.hex()
            )
            .into(),
        ));
    }
    validate_commit_proto_structure(&mut proto_commit)?;
    let is_root = expected_id.as_bytes() == cc_common::ROOT_COMMIT_ID_BYTES;
    if !is_root && proto_commit.parent_commit_ids.is_empty() {
        return Err(BackendError::Other(
            "ReadCommit non-root commit must have at least one parent".into(),
        ));
    }
    let has_root_parent = proto_commit
        .parent_commit_ids
        .iter()
        .any(|pid| pid.as_slice() == cc_common::ROOT_COMMIT_ID_BYTES);
    if has_root_parent && proto_commit.parent_commit_ids.len() > 1 {
        return Err(BackendError::Other(
            "ReadCommit ROOT_COMMIT_ID may only appear as the sole parent".into(),
        ));
    }
    validate_external_ids(&proto_commit.external_ids)?;

    let verified_sig = if !is_root && !proto_commit.raw_git_commit.is_empty() {
        let raw_hash = gix::objs::compute_hash(
            gix::hash::Kind::Sha1,
            gix::objs::Kind::Commit,
            &proto_commit.raw_git_commit,
        )
        .map_err(|e| BackendError::Other(Box::new(e)))?;
        if raw_hash.as_bytes() != expected_id.as_bytes() {
            return Err(BackendError::Other(
                "ReadCommit raw_git_commit digest does not match commit_id".into(),
            ));
        }
        let parsed =
            gix::objs::CommitRef::from_bytes(&proto_commit.raw_git_commit, gix::hash::Kind::Sha1)
                .map_err(|e| {
                BackendError::Other(format!("invalid raw_git_commit bytes: {e}").into())
            })?;
        verify_raw_git_commit_matches_proto(&proto_commit, &parsed, expected_id)?;

        let raw_sig = gix::objs::CommitRefIter::signature(
            &proto_commit.raw_git_commit,
            gix::hash::Kind::Sha1,
        )
        .map_err(|e| BackendError::Other(Box::new(e)))?;

        match (raw_sig, proto_commit.secure_sig.take()) {
            (Some((raw_sig_bytes, raw_signed_data)), Some(mut sig)) => {
                if sig.sig.is_empty() {
                    return Err(BackendError::Other(
                        "ReadCommit secure_sig.sig must not be empty".into(),
                    ));
                }
                if raw_sig_bytes.as_ref() != sig.sig.as_slice() {
                    return Err(BackendError::Other(
                        "ReadCommit secure_sig.sig does not match raw_git_commit gpgsig".into(),
                    ));
                }
                let canonical_data = Vec::from(raw_signed_data.to_bstring());
                if !sig.data.is_empty() && sig.data != canonical_data {
                    return Err(BackendError::Other(
                        "ReadCommit secure_sig.data does not match raw_git_commit payload".into(),
                    ));
                }
                sig.data = canonical_data;
                Some(SecureSig {
                    data: sig.data,
                    sig: sig.sig,
                })
            }
            (None, None) => None,
            (Some(_), None) => {
                return Err(BackendError::Other(
                    "ReadCommit raw_git_commit has gpgsig header but commit.secure_sig is missing"
                        .into(),
                ));
            }
            (None, Some(_)) => {
                return Err(BackendError::Other(
                    "ReadCommit commit.secure_sig is set but raw_git_commit lacks gpgsig header"
                        .into(),
                ));
            }
        }
    } else if !is_root {
        let unsigned_data = canonical_git_commit_bytes(&proto_commit, None)?;
        if let Some(mut sig) = proto_commit.secure_sig.take() {
            if sig.sig.is_empty() {
                return Err(BackendError::Other(
                    "ReadCommit secure_sig.sig must not be empty".into(),
                ));
            }
            let signed_bytes = canonical_git_commit_bytes(&proto_commit, Some(&sig.sig))?;
            let computed = gix::objs::compute_hash(
                gix::hash::Kind::Sha1,
                gix::objs::Kind::Commit,
                &signed_bytes,
            )
            .map_err(|e| BackendError::Other(Box::new(e)))?;
            if computed.as_bytes() != expected_id.as_bytes() {
                return Err(BackendError::Other(
                    "ReadCommit signed commit digest does not match commit_id".into(),
                ));
            }
            if !sig.data.is_empty() && sig.data != unsigned_data {
                return Err(BackendError::Other(
                    "ReadCommit secure_sig.data does not match canonical commit payload".into(),
                ));
            }
            sig.data = unsigned_data;
            Some(SecureSig {
                data: sig.data,
                sig: sig.sig,
            })
        } else {
            let computed = gix::objs::compute_hash(
                gix::hash::Kind::Sha1,
                gix::objs::Kind::Commit,
                &unsigned_data,
            )
            .map_err(|e| BackendError::Other(Box::new(e)))?;
            if computed.as_bytes() != expected_id.as_bytes() {
                return Err(BackendError::Other(
                    "ReadCommit canonical commit digest does not match commit_id".into(),
                ));
            }
            None
        }
    } else {
        proto_commit.secure_sig = None;
        None
    };

    let mut commit = commit_from_proto(proto_commit)?;
    commit.secure_sig = verified_sig;
    Ok(commit)
}

/// Converts a protobuf `Commit` into a `jj_lib::backend::Commit`, validating structural
/// invariants so malformed payloads return `Err` instead of panicking.
///
/// Always clears `secure_sig` because `SecureSig.data` is not verified against canonical
/// commit bytes here; callers that expose commit signatures must go through
/// `verified_commit_from_proto`.
pub fn commit_from_proto(mut proto_commit: cc_common::backend::Commit) -> BackendResult<Commit> {
    validate_commit_proto_structure(&mut proto_commit)?;
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

    Ok(Commit {
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
    })
}
