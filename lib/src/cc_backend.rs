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

//! Client-side `jj_lib::backend::Backend` implementation over `BackendService`.
//!
//! # Cross-Process and RPC Boundaries
//! Reconstructs and verifies `SecureSig.data` from canonical Git commit bytes or a
//! digest-verified `raw_git_commit` before surfacing signatures to `jj_lib`, rejects NUL bytes
//! and digest mismatches on `ReadSymlink` responses, and verifies strict ascending
//! `RepoPathComponent` order on `ReadTree` responses before calling `Tree::from_sorted_entries`.

#![allow(elided_lifetimes_in_paths)]

use async_trait::async_trait;
use futures::StreamExt as _;
use futures::stream::{self, BoxStream};
use jj_lib::backend::*;
use jj_lib::object_id::ObjectId;
use jj_lib::repo_path::{RepoPath, RepoPathBuf};
use std::fmt::Debug;
use std::fs;
use std::path::Path;
use std::pin::Pin;
use std::time::SystemTime;

use crate::client::connect_backend_client;
use crate::util::{CommitCloudConfig, run_async};

#[derive(Debug)]
pub struct CommitCloudBackend {
    config: CommitCloudConfig,
    root_commit_id: CommitId,
    root_change_id: ChangeId,
    empty_tree_id: TreeId,
}

impl CommitCloudBackend {
    pub fn name() -> &'static str {
        "commit_cloud"
    }

    pub fn init(
        store_path: &Path,
        server_url: &str,
        project_id: &str,
        explicit_repo_id: Option<&str>,
    ) -> Result<Self, Box<dyn std::error::Error + Send + Sync>> {
        let root_commit_id = CommitId::from_bytes(&cc_common::ROOT_COMMIT_ID_BYTES);
        let root_change_id = ChangeId::from_bytes(&cc_common::ROOT_CHANGE_ID_BYTES);
        let empty_tree_id = TreeId::from_hex(cc_common::EMPTY_TREE_ID_HEX);

        let initial_config = CommitCloudConfig {
            server_url: server_url.to_string(),
            project_id: project_id.to_string(),
            repo_id: String::new(),
            use_daemon: true,
            daemon_socket: None,
        };

        let project_id_cloned = project_id.to_string();
        let repo_id_opt = explicit_repo_id.map(|s| s.to_string());
        let initial_config_clone = initial_config.clone();
        let (project_id, repo_id) = run_async(move || async move {
            let mut client = connect_backend_client(&initial_config_clone).await?;
            let register_repo_response = client
                .register_repository(tonic::Request::new(
                    cc_common::backend::RegisterRepositoryRequest {
                        project_id: project_id_cloned,
                        repo_id: repo_id_opt,
                        name: None,
                        hash_algorithm: cc_common::backend::HashAlgorithm::Sha1 as i32,
                    },
                ))
                .await?
                .into_inner();
            Ok((
                register_repo_response.project_id,
                register_repo_response.repo_id,
            ))
        })?;

        let config_path = store_path.join("config.toml");
        let config = CommitCloudConfig {
            server_url: server_url.to_string(),
            project_id,
            repo_id,
            use_daemon: true,
            daemon_socket: None,
        };
        fs::write(&config_path, toml::to_string_pretty(&config)?)?;

        Ok(Self {
            config,
            root_commit_id,
            root_change_id,
            empty_tree_id,
        })
    }

    pub fn load(store_path: &Path) -> Result<Self, Box<dyn std::error::Error + Send + Sync>> {
        let config = CommitCloudConfig::load_from_store(store_path)?;
        let root_commit_id = CommitId::from_bytes(&cc_common::ROOT_COMMIT_ID_BYTES);
        let root_change_id = ChangeId::from_bytes(&cc_common::ROOT_CHANGE_ID_BYTES);
        let empty_tree_id = TreeId::from_hex(cc_common::EMPTY_TREE_ID_HEX);
        Ok(Self {
            config,
            root_commit_id,
            root_change_id,
            empty_tree_id,
        })
    }
}

pub use cc_common::conversions::backend::*;

const EMPTY_STRING_PLACEHOLDER: &str = "JJ_EMPTY_STRING";

fn canonical_git_commit_bytes(
    proto: &cc_common::backend::Commit,
    sig_override: Option<&[u8]>,
) -> BackendResult<Vec<u8>> {
    const REVERSE_HEX_CHARS: &[u8; 16] = b"zyxwvutsrqponmlk";
    const FORWARD_HEX_CHARS: &[u8; 16] = b"0123456789abcdef";

    use gix::objs::WriteTo as _;
    let tree_id = match proto.root_tree_id.first() {
        Some(id) => gix::hash::ObjectId::try_from(id.as_slice()).map_err(|e| {
            BackendError::Other(format!("invalid root tree id for signing: {e}").into())
        })?,
        None => {
            return Err(BackendError::Other(
                "commit must have at least one root tree".into(),
            ));
        }
    };
    let mut parents = Vec::with_capacity(proto.parent_commit_ids.len());
    for id in &proto.parent_commit_ids {
        if id.as_slice() == cc_common::ROOT_COMMIT_ID_BYTES {
            continue;
        }
        parents.push(gix::hash::ObjectId::try_from(id.as_slice()).map_err(|e| {
            BackendError::Other(format!("invalid parent commit id for signing: {e}").into())
        })?);
    }

    let sig_to_git =
        |sig: Option<&cc_common::backend::Signature>| -> BackendResult<gix::actor::Signature> {
            let (name, email, millis, tz_offset) = match sig {
                Some(s) => {
                    let ts = s.timestamp.as_ref();
                    (
                        s.name.as_str(),
                        s.email.as_str(),
                        ts.map_or(0, |t| t.millis_since_epoch),
                        ts.map_or(0, |t| t.tz_offset),
                    )
                }
                None => ("", "", 0, 0),
            };
            let tz_secs = tz_offset.checked_mul(60).ok_or_else(|| {
                BackendError::Other(
                    format!("signature timezone offset {tz_offset} minutes is out of range").into(),
                )
            })?;
            if tz_secs.unsigned_abs() / 3600 > 99 {
                return Err(BackendError::Other(
                    format!("signature timezone offset {tz_offset} minutes exceeds Git limits")
                        .into(),
                ));
            }
            let name = if name.is_empty() {
                EMPTY_STRING_PLACEHOLDER
            } else {
                name
            };
            let email = if email.is_empty() {
                EMPTY_STRING_PLACEHOLDER
            } else {
                email
            };
            Ok(gix::actor::Signature {
                name: name.into(),
                email: email.into(),
                time: gix::date::Time {
                    seconds: millis.div_euclid(1000),
                    offset: tz_secs,
                },
            })
        };

    let mut extra_headers = Vec::new();
    let has_non_empty_label = proto.conflict_labels.iter().any(|l| !l.is_empty());
    if proto.root_tree_id.len() > 1 && proto.conflict_labels.len() > 1 && has_non_empty_label {
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
        return Ok(());
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
        if value.is_empty() || value.len() > 64 {
            return Err(BackendError::Other(
                format!(
                    "ReadCommit external_ids[{key:?}] must be 1..=64 bytes (got {} bytes)",
                    value.len()
                )
                .into(),
            ));
        }
        match key.as_str() {
            "git-sha1" if value.len() != 20 => {
                return Err(BackendError::Other(
                    format!(
                        "ReadCommit external_ids['git-sha1'] must be 20 bytes (got {} bytes)",
                        value.len()
                    )
                    .into(),
                ));
            }
            "git-sha256" if value.len() != 32 => {
                return Err(BackendError::Other(
                    format!(
                        "ReadCommit external_ids['git-sha256'] must be 32 bytes (got {} bytes)",
                        value.len()
                    )
                    .into(),
                ));
            }
            _ => {}
        }
    }
    Ok(())
}

fn verified_commit_from_proto(
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
    let is_root = expected_id.as_bytes() == cc_common::ROOT_COMMIT_ID_BYTES;
    if !is_root && proto_commit.parent_commit_ids.is_empty() {
        return Err(BackendError::Other(
            "ReadCommit non-root commit must have at least one parent".into(),
        ));
    }
    let mut has_root_parent = false;
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
        if pid.as_slice() == cc_common::ROOT_COMMIT_ID_BYTES {
            has_root_parent = true;
        }
    }
    if has_root_parent && proto_commit.parent_commit_ids.len() > 1 {
        return Err(BackendError::Other(
            "ReadCommit ROOT_COMMIT_ID may only appear as the sole parent".into(),
        ));
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

    let mut commit = commit_from_proto(proto_commit);
    commit.secure_sig = verified_sig;
    Ok(commit)
}

fn verified_symlink_target_from_proto(
    expected_id: &SymlinkId,
    target: String,
) -> BackendResult<String> {
    if target.contains('\0') {
        return Err(BackendError::Other(
            "ReadSymlink target must not contain NUL bytes".into(),
        ));
    }
    let computed = gix::objs::compute_hash(
        gix::hash::Kind::Sha1,
        gix::objs::Kind::Blob,
        target.as_bytes(),
    )
    .map_err(|e| BackendError::Other(Box::new(e)))?;
    if computed.as_bytes() != expected_id.as_bytes() {
        return Err(BackendError::Other(
            format!(
                "ReadSymlink target digest {} does not match requested {}",
                computed.to_hex(),
                expected_id.hex()
            )
            .into(),
        ));
    }
    Ok(target)
}

#[async_trait]
impl Backend for CommitCloudBackend {
    fn name(&self) -> &str {
        Self::name()
    }

    fn commit_id_length(&self) -> usize {
        cc_common::COMMIT_ID_LENGTH
    }

    fn change_id_length(&self) -> usize {
        cc_common::CHANGE_ID_LENGTH
    }

    fn root_commit_id(&self) -> &CommitId {
        &self.root_commit_id
    }

    fn root_change_id(&self) -> &ChangeId {
        &self.root_change_id
    }

    fn empty_tree_id(&self) -> &TreeId {
        &self.empty_tree_id
    }

    fn concurrency(&self) -> usize {
        1
    }

    async fn read_file(
        &self,
        _path: &RepoPath,
        id: &FileId,
    ) -> BackendResult<Pin<Box<dyn futures::AsyncRead + Send>>> {
        let config = self.config.clone();
        let project_id = self.config.project_id.clone();
        let file_id_bytes = id.to_bytes().to_vec();
        let file_id_hex = id.hex();

        let content = run_async(move || async move {
            let mut client = connect_backend_client(&config).await?;
            let res = client
                .read_file(tonic::Request::new(cc_common::backend::ReadFileRequest {
                    project_id,
                    file_id: file_id_bytes,
                    offset: 0,
                    max_bytes: 0,
                }))
                .await;

            let mut stream = match res {
                Ok(r) => r.into_inner(),
                Err(status) if status.code() == tonic::Code::NotFound => {
                    return Err(Box::new(BackendError::ObjectNotFound {
                        object_type: "file".into(),
                        hash: file_id_hex,
                        source: Some(status.into()),
                    })
                        as Box<dyn std::error::Error + Send + Sync>);
                }
                Err(status) => {
                    return Err(Box::new(status) as Box<dyn std::error::Error + Send + Sync>);
                }
            };

            let mut content = Vec::new();
            while let Some(chunk) = stream.next().await {
                let chunk = chunk.map_err(|e| BackendError::Other(e.into()))?;
                content.extend_from_slice(&chunk.chunk);
            }
            Ok(content)
        })
        .map_err(|e| match e.downcast::<BackendError>() {
            Ok(err) => *err,
            Err(e) => BackendError::Other(e),
        })?;

        let cursor = futures::io::Cursor::new(content);
        Ok(Box::pin(cursor) as Pin<Box<dyn futures::AsyncRead + Send>>)
    }

    async fn write_file(
        &self,
        _path: &RepoPath,
        contents: &mut (dyn futures::AsyncRead + Send + Unpin),
    ) -> BackendResult<FileId> {
        let mut buffer = Vec::new();
        futures::AsyncReadExt::read_to_end(contents, &mut buffer)
            .await
            .map_err(|e| BackendError::Other(e.into()))?;

        let config = self.config.clone();
        let project_id = self.config.project_id.clone();

        let file_id_bytes = run_async(move || async move {
            let mut client = connect_backend_client(&config).await?;
            let res = client
                .write_file(tonic::Request::new(cc_common::backend::WriteFileRequest {
                    project_id,
                    content: buffer,
                }))
                .await?;
            Ok(res.into_inner().file_id)
        })
        .map_err(|e| BackendError::Other(e.into()))?;

        if file_id_bytes.len() != cc_common::FILE_ID_LENGTH {
            return Err(BackendError::Other(
                format!(
                    "WriteFile returned invalid file_id length {}",
                    file_id_bytes.len()
                )
                .into(),
            ));
        }
        Ok(FileId::from_bytes(&file_id_bytes))
    }

    async fn read_symlink(&self, _path: &RepoPath, id: &SymlinkId) -> BackendResult<String> {
        let config = self.config.clone();
        let project_id = self.config.project_id.clone();
        let symlink_id_bytes = id.to_bytes().to_vec();
        let symlink_id_hex = id.hex();

        let target = run_async(move || async move {
            let mut client = connect_backend_client(&config).await?;
            let res = client
                .read_symlink(tonic::Request::new(
                    cc_common::backend::ReadSymlinkRequest {
                        project_id,
                        symlink_id: symlink_id_bytes,
                    },
                ))
                .await;
            match res {
                Ok(r) => Ok(r.into_inner().target),
                Err(status) if status.code() == tonic::Code::NotFound => {
                    Err(Box::new(BackendError::ObjectNotFound {
                        object_type: "symlink".into(),
                        hash: symlink_id_hex,
                        source: Some(status.into()),
                    })
                        as Box<dyn std::error::Error + Send + Sync>)
                }
                Err(status) => Err(Box::new(status) as Box<dyn std::error::Error + Send + Sync>),
            }
        })
        .map_err(|e| match e.downcast::<BackendError>() {
            Ok(err) => *err,
            Err(e) => BackendError::Other(e),
        })?;

        verified_symlink_target_from_proto(id, target)
    }

    async fn write_symlink(&self, _path: &RepoPath, target: &str) -> BackendResult<SymlinkId> {
        let config = self.config.clone();
        let project_id = self.config.project_id.clone();
        let target = target.to_string();

        let symlink_id_bytes = run_async(move || async move {
            let mut client = connect_backend_client(&config).await?;
            let res = client
                .write_symlink(tonic::Request::new(
                    cc_common::backend::WriteSymlinkRequest { project_id, target },
                ))
                .await?;
            Ok(res.into_inner().symlink_id)
        })
        .map_err(|e| BackendError::Other(e.into()))?;

        if symlink_id_bytes.len() != cc_common::SYMLINK_ID_LENGTH {
            return Err(BackendError::Other(
                format!(
                    "WriteSymlink returned invalid symlink_id length {}",
                    symlink_id_bytes.len()
                )
                .into(),
            ));
        }
        Ok(SymlinkId::from_bytes(&symlink_id_bytes))
    }

    async fn read_copy(&self, _id: &CopyId) -> BackendResult<CopyHistory> {
        Err(BackendError::Unsupported(
            "copies not supported".to_string(),
        ))
    }

    async fn write_copy(&self, _contents: &CopyHistory) -> BackendResult<CopyId> {
        Err(BackendError::Unsupported(
            "copies not supported".to_string(),
        ))
    }

    async fn get_related_copies(&self, _copy_id: &CopyId) -> BackendResult<Vec<RelatedCopy>> {
        Err(BackendError::Unsupported(
            "copies not supported".to_string(),
        ))
    }

    async fn read_tree(&self, path: &RepoPath, id: &TreeId) -> BackendResult<Tree> {
        if *id == self.empty_tree_id {
            return Ok(Tree::from_sorted_entries(vec![]));
        }

        let config = self.config.clone();
        let project_id = self.config.project_id.clone();
        let tree_id_bytes = id.to_bytes().to_vec();
        let tree_id_hex = id.hex();
        let path_str = path.as_internal_file_string().to_string();

        let proto_entries = run_async(move || async move {
            let mut client = connect_backend_client(&config).await?;
            let mut all_entries = Vec::new();
            let mut page_token = String::new();
            loop {
                let res = client
                    .read_tree(tonic::Request::new(cc_common::backend::ReadTreeRequest {
                        project_id: project_id.clone(),
                        path: path_str.clone(),
                        tree_id: tree_id_bytes.clone(),
                        page_size: 0,
                        page_token: page_token.clone(),
                    }))
                    .await;
                let resp = match res {
                    Ok(r) => r.into_inner(),
                    Err(status) if status.code() == tonic::Code::NotFound => {
                        return Err(Box::new(BackendError::ObjectNotFound {
                            object_type: "tree".into(),
                            hash: tree_id_hex,
                            source: Some(status.into()),
                        })
                            as Box<dyn std::error::Error + Send + Sync>);
                    }
                    Err(status) => {
                        return Err(Box::new(status) as Box<dyn std::error::Error + Send + Sync>);
                    }
                };
                let page_empty = resp.entries.is_empty();
                all_entries.extend(resp.entries);
                if resp.next_page_token.is_empty() {
                    break;
                }
                if page_empty {
                    return Err(
                        "ReadTree returned empty page with non-empty next_page_token".into(),
                    );
                }
                if !page_token.is_empty() && resp.next_page_token <= page_token {
                    return Err("ReadTree returned non-advancing next_page_token".into());
                }
                page_token = resp.next_page_token;
            }
            Ok(all_entries)
        })
        .map_err(|e| match e.downcast::<BackendError>() {
            Ok(err) => *err,
            Err(e) => BackendError::Other(e),
        })?;

        let mut entries: Vec<(jj_lib::repo_path::RepoPathComponentBuf, TreeValue)> =
            Vec::with_capacity(proto_entries.len());
        for proto_entry in proto_entries {
            let (name, value) =
                tree_entry_from_proto(proto_entry).map_err(|e| BackendError::Other(e.into()))?;
            if let Some((prev_name, _)) = entries.last()
                && *prev_name >= name
            {
                return Err(BackendError::Other(
                    format!(
                        "ReadTree entries not in strict ascending order: {:?} >= {:?}",
                        prev_name.as_internal_str(),
                        name.as_internal_str()
                    )
                    .into(),
                ));
            }
            entries.push((name, value));
        }

        Ok(Tree::from_sorted_entries(entries))
    }

    async fn write_tree(&self, path: &RepoPath, contents: &Tree) -> BackendResult<TreeId> {
        if contents.entries().next().is_none() {
            return Ok(self.empty_tree_id.clone());
        }

        let proto_entries: Result<Vec<_>, _> = contents
            .entries()
            .map(|e| tree_entry_to_proto(&e))
            .collect();
        let proto_entries = proto_entries?;

        let config = self.config.clone();
        let project_id = self.config.project_id.clone();
        let path_str = path.as_internal_file_string().to_string();

        let tree_id_bytes = run_async(move || async move {
            let mut client = connect_backend_client(&config).await?;
            let res = client
                .write_tree(tonic::Request::new(cc_common::backend::WriteTreeRequest {
                    project_id,
                    path: path_str,
                    entries: proto_entries,
                }))
                .await?;
            Ok(res.into_inner().tree_id)
        })
        .map_err(|e| BackendError::Other(e.into()))?;

        if tree_id_bytes.len() != cc_common::TREE_ID_LENGTH {
            return Err(BackendError::Other(
                format!(
                    "WriteTree returned invalid tree_id length {}",
                    tree_id_bytes.len()
                )
                .into(),
            ));
        }
        Ok(TreeId::from_bytes(&tree_id_bytes))
    }

    async fn read_commit(&self, id: &CommitId) -> BackendResult<Commit> {
        if *id == self.root_commit_id {
            return Ok(make_root_commit(
                self.root_change_id.clone(),
                self.empty_tree_id.clone(),
            ));
        }

        let config = self.config.clone();
        let project_id = self.config.project_id.clone();
        let commit_id_bytes = id.to_bytes().to_vec();
        let commit_id_hex = id.hex();

        let proto_commit = run_async(move || async move {
            let mut client = connect_backend_client(&config).await?;
            let res = client
                .read_commit(tonic::Request::new(cc_common::backend::ReadCommitRequest {
                    project_id,
                    commit_id: commit_id_bytes,
                }))
                .await;

            match res {
                Ok(r) => r
                    .into_inner()
                    .commit
                    .ok_or_else(|| "ReadCommit response missing commit payload".into()),
                Err(status) if status.code() == tonic::Code::NotFound => {
                    Err(Box::new(BackendError::ObjectNotFound {
                        object_type: "commit".into(),
                        hash: commit_id_hex,
                        source: Some(status.into()),
                    })
                        as Box<dyn std::error::Error + Send + Sync>)
                }
                Err(status) => Err(Box::new(status) as Box<dyn std::error::Error + Send + Sync>),
            }
        })
        .map_err(|e| match e.downcast::<BackendError>() {
            Ok(err) => *err,
            Err(e) => BackendError::Other(e),
        })?;

        verified_commit_from_proto(id, proto_commit)
    }

    async fn write_commit(
        &self,
        mut commit: Commit,
        sign_with: Option<&mut SigningFn>,
    ) -> BackendResult<(CommitId, Commit)> {
        if commit.parents.contains(&self.root_commit_id) && commit.parents.len() > 1 {
            return Err(BackendError::Unsupported(
                "The Commit Cloud backend does not support creating merge commits with the root \
                 commit as one of the parents."
                    .to_owned(),
            ));
        }

        apply_commit_signing(&mut commit, sign_with)?;
        let proto_commit = commit_to_proto(&commit);
        let config = self.config.clone();
        let project_id = self.config.project_id.clone();

        let returned_id_bytes = run_async(move || async move {
            let mut client = connect_backend_client(&config).await?;
            let res = client
                .write_commit(tonic::Request::new(
                    cc_common::backend::WriteCommitRequest {
                        project_id,
                        commit: Some(proto_commit),
                    },
                ))
                .await?;
            Ok(res.into_inner().commit_id)
        })
        .map_err(|e| BackendError::Other(e.into()))?;

        if returned_id_bytes.len() != cc_common::COMMIT_ID_LENGTH {
            return Err(BackendError::Other(
                format!(
                    "WriteCommit returned invalid commit_id length {}",
                    returned_id_bytes.len()
                )
                .into(),
            ));
        }
        let returned_id = CommitId::from_bytes(&returned_id_bytes);
        Ok((returned_id, commit))
    }

    fn get_copy_records(
        &self,
        _paths: Option<&[RepoPathBuf]>,
        _root: &CommitId,
        _head: &CommitId,
    ) -> BackendResult<BoxStream<'_, BackendResult<CopyRecord>>> {
        Ok(stream::empty().boxed())
    }

    async fn gc(&self, _heads: &[CommitId], _keep_newer: SystemTime) -> BackendResult<()> {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

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

        // Raw hand-formatted Git commit with multi-line `gpgsig` header also round-trips cleanly.
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
    fn verified_commit_from_proto_binds_synthetic_change_id_and_validates_external_ids() {
        use gix::objs::WriteTo as _;

        // Build a raw Git commit without a `change-id` header.
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
        labeled_proto.conflict_labels =
            vec!["left".to_string(), String::new(), "right".to_string()];
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
        let (single_id, single_raw) =
            make_raw_with_headers(vec![("jj:conflict-labels", b"\n\n\n")]);
        let mut single_commit = make_base_commit();
        single_commit.root_tree = jj_lib::merge::Merge::resolved(TreeId::from_bytes(&trees[0]));
        single_commit.change_id =
            jj_lib::git_backend::synthetic_change_id_from_git_commit_id(&single_id);
        let mut single_proto = commit_to_proto(&single_commit);
        single_proto.raw_git_commit = single_raw;
        assert!(verified_commit_from_proto(&single_id, single_proto).is_err());
    }

    #[test]
    fn verified_symlink_target_from_proto_checks_nul_and_blob_digest() {
        let target = "dir/target.txt".to_string();
        let expected_id = SymlinkId::from_bytes(
            gix::objs::compute_hash(
                gix::hash::Kind::Sha1,
                gix::objs::Kind::Blob,
                target.as_bytes(),
            )
            .unwrap()
            .as_bytes(),
        );
        assert_eq!(
            verified_symlink_target_from_proto(&expected_id, target.clone()).unwrap(),
            target
        );

        // Digest mismatch must fail closed.
        assert!(
            verified_symlink_target_from_proto(&expected_id, "dir/other.txt".to_string()).is_err()
        );

        // Embedded NUL byte must be rejected even if the digest matches the NUL-containing payload.
        let nul_target = "dir/bad\0target".to_string();
        let nul_id = SymlinkId::from_bytes(
            gix::objs::compute_hash(
                gix::hash::Kind::Sha1,
                gix::objs::Kind::Blob,
                nul_target.as_bytes(),
            )
            .unwrap()
            .as_bytes(),
        );
        assert!(verified_symlink_target_from_proto(&nul_id, nul_target).is_err());
    }
}
