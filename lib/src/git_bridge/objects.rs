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

//! Git object validation, commit parsing, and proto-to-Git commit serialization
//! preserving 1:1 20-byte SHA-1 digests across import and export.
//!
//! # Authority and Crash-Safety Ordering
//! Commit export prefers verified `raw_git_commit` bytes and checks that parsed
//! headers, tree, parents, and timestamps match the proto `Commit` before writing
//! to `.git/objects/`, preventing silent corruption or header loss on round-trips.
//!
//! # Concurrency and Merge Invariants
//! Tree entry names and Git file modes are validated strictly on both directions.
//! Conflicted `jj` commits encode extra trees and labels in `jj:trees` and
//! `jj:conflict-labels` headers and decode `change-id` via reverse-hex encoding.

use cc_common::backend::{Commit, SecureSig, Signature, Timestamp};
use cc_common::{CHANGE_ID_LENGTH, COMMIT_ID_LENGTH, ROOT_COMMIT_ID_BYTES, TREE_ID_LENGTH};
use gix::objs::WriteTo as _;
use gix::prelude::Write as _;
use jj_lib::hex_util::{decode_reverse_hex, encode_reverse_hex};

use super::EMPTY_STRING_PLACEHOLDER;

pub(super) fn validate_tree_entry_name(
    tree_oid: gix::hash::ObjectId,
    filename: &[u8],
) -> Result<&str, Box<dyn std::error::Error + Send + Sync>> {
    let name = std::str::from_utf8(filename).map_err(|e| {
        format!(
            "Non-UTF-8 filename {:?} in Git tree {tree_oid}: {e}",
            gix::bstr::BStr::new(filename)
        )
    })?;
    if name.is_empty() || name == "." || name == ".." || name.contains(['/', '\0']) {
        return Err(format!("Invalid filename {name:?} in Git tree {tree_oid}").into());
    }
    Ok(name)
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum GitTreeEntryKind {
    Tree,
    Blob { executable: bool },
    Link,
    Submodule,
}

pub(super) fn validate_git_tree_mode(
    tree_oid: gix::hash::ObjectId,
    name: &str,
    mode: gix::objs::tree::EntryMode,
) -> Result<GitTreeEntryKind, Box<dyn std::error::Error + Send + Sync>> {
    match mode.value() {
        0o040000 => Ok(GitTreeEntryKind::Tree),
        0o100644 => Ok(GitTreeEntryKind::Blob { executable: false }),
        0o100755 => Ok(GitTreeEntryKind::Blob { executable: true }),
        0o120000 => Ok(GitTreeEntryKind::Link),
        0o160000 => Ok(GitTreeEntryKind::Submodule),
        raw => Err(format!(
            "Unsupported Git tree entry mode {raw:06o} for {name:?} in Git tree {tree_oid}"
        )
        .into()),
    }
}

/// Parses the first `jj:trees` commit header into an odd-length list (`>= 3`) of SHA-1 tree IDs.
///
/// Returns `Ok(None)` when the commit carries no `jj:trees` header, and `Err(...)` whenever the
/// header is present but malformed (non-space separators, empty tokens, non-SHA-1 OIDs, or a
/// non-conflict / even term count).
pub(super) fn parse_jj_trees_header(
    commit: &gix::objs::CommitRef<'_>,
) -> Result<Option<Vec<gix::hash::ObjectId>>, Box<dyn std::error::Error + Send + Sync>> {
    let Some(val) = commit.extra_headers().find("jj:trees") else {
        return Ok(None);
    };
    let raw_trees: &[u8] = val.as_ref();
    let mut ids = Vec::new();
    for hex_tok in raw_trees.split(|&b| b == b' ') {
        if hex_tok.is_empty() {
            return Err("Invalid `jj:trees` header: empty tree ID token".into());
        }
        let oid = gix::hash::ObjectId::from_hex(hex_tok).map_err(|e| {
            format!(
                "Invalid `jj:trees` header: invalid tree ID {:?}: {e}",
                gix::bstr::BStr::new(hex_tok)
            )
        })?;
        if oid.kind() != gix::hash::Kind::Sha1 {
            return Err(format!(
                "Invalid `jj:trees` header: expected SHA-1 tree ID, got {:?} ({oid})",
                oid.kind()
            )
            .into());
        }
        ids.push(oid);
    }
    if ids.len() <= 1 || ids.len().is_multiple_of(2) {
        return Err(format!(
            "Invalid `jj:trees` header: expected an odd number of at least 3 trees, got {}",
            ids.len()
        )
        .into());
    }
    Ok(Some(ids))
}

fn parse_jj_conflict_labels_header(
    commit: &gix::objs::CommitRef<'_>,
    num_trees: usize,
) -> Result<Vec<String>, Box<dyn std::error::Error + Send + Sync>> {
    let Some(val) = commit.extra_headers().find("jj:conflict-labels") else {
        return Ok(Vec::new());
    };
    let s = std::str::from_utf8(val.as_ref())
        .map_err(|e| format!("Invalid `jj:conflict-labels` header: not valid UTF-8: {e}"))?;
    let labels: Vec<String> = s.split_terminator('\n').map(ToOwned::to_owned).collect();
    if labels.is_empty() {
        return Ok(labels);
    }
    if num_trees <= 1 {
        return Err("Invalid `jj:conflict-labels` header: present without `jj:trees`".into());
    }
    if labels.len() != num_trees {
        return Err(format!(
            "Invalid `jj:conflict-labels` header: expected {num_trees} labels, got {}",
            labels.len()
        )
        .into());
    }
    if labels.iter().all(|l| l.is_empty()) {
        return Err("Invalid `jj:conflict-labels` header: all labels are empty".into());
    }
    Ok(labels)
}

pub(super) struct ParsedGitCommit {
    pub(super) raw_commit_bytes: Vec<u8>,
    pub(super) tree_oid: gix::hash::ObjectId,
    pub(super) parent_ids: Vec<Vec<u8>>,
    pub(super) author: Signature,
    pub(super) committer: Signature,
    pub(super) message: String,
    pub(super) parsed_change_id: Option<Vec<u8>>,
    pub(super) extra_tree_ids: Vec<gix::hash::ObjectId>,
    pub(super) conflict_labels: Vec<String>,
    pub(super) secure_sig: Option<SecureSig>,
}

pub(super) fn parse_git_commit(
    repo: &gix::Repository,
    commit_oid: gix::hash::ObjectId,
) -> Result<ParsedGitCommit, Box<dyn std::error::Error + Send + Sync>> {
    let object = repo.find_object(commit_oid)?;
    let raw_commit_bytes = object.data.to_vec();
    let commit_ref = object
        .try_to_commit_ref()
        .map_err(|e| format!("Failed to decode Git commit {commit_oid}: {e}"))?;

    let tree_oid = commit_ref.tree();
    let author_sig = commit_ref
        .author()
        .map_err(|e| format!("Invalid Git author in {commit_oid}: {e}"))?;
    let author_time = author_sig
        .time()
        .map_err(|e| format!("Invalid Git author timestamp in {commit_oid}: {e}"))?;
    let committer_sig = commit_ref
        .committer()
        .map_err(|e| format!("Invalid Git committer in {commit_oid}: {e}"))?;
    let committer_time = committer_sig
        .time()
        .map_err(|e| format!("Invalid Git committer timestamp in {commit_oid}: {e}"))?;
    let parent_ids: Vec<Vec<u8>> = commit_ref
        .parents()
        .map(|p| p.as_bytes().to_vec())
        .collect();

    let parsed_change_id = commit_ref
        .extra_headers()
        .find("change-id")
        .and_then(|v| decode_change_id_header(v.as_ref()));

    let extra_tree_ids = parse_jj_trees_header(&commit_ref)
        .map_err(|e| format!("Failed to parse Git commit {commit_oid}: {e}"))?
        .unwrap_or_default();

    let num_trees = if extra_tree_ids.len() > 1 {
        extra_tree_ids.len()
    } else {
        1
    };
    let conflict_labels = parse_jj_conflict_labels_header(&commit_ref, num_trees)
        .map_err(|e| format!("Failed to parse Git commit {commit_oid}: {e}"))?;

    let secure_sig =
        match gix::objs::CommitRefIter::signature(&raw_commit_bytes, gix::hash::Kind::Sha1)
            .map_err(|e| format!("Invalid Git signature header in {commit_oid}: {e}"))?
        {
            Some((sig, _)) => {
                if sig.is_empty() {
                    return Err(
                        format!("Empty Git signature header (gpgsig) in {commit_oid}").into(),
                    );
                }
                Some(SecureSig {
                    data: Vec::new(),
                    sig: sig.as_ref().to_vec(),
                })
            }
            None => None,
        };

    let clean_git_str = |raw: &gix::bstr::BStr| -> String {
        let s = raw.to_string();
        if s == EMPTY_STRING_PLACEHOLDER {
            String::new()
        } else {
            s
        }
    };

    let author = Signature {
        name: clean_git_str(author_sig.name),
        email: clean_git_str(author_sig.email),
        timestamp: Some(Timestamp {
            millis_since_epoch: author_time
                .seconds
                .checked_mul(1000)
                .ok_or_else(|| format!("Git author timestamp out of range in {commit_oid}"))?,
            tz_offset: author_time.offset.div_euclid(60),
        }),
    };
    let committer = Signature {
        name: clean_git_str(committer_sig.name),
        email: clean_git_str(committer_sig.email),
        timestamp: Some(Timestamp {
            millis_since_epoch: committer_time
                .seconds
                .checked_mul(1000)
                .ok_or_else(|| format!("Git committer timestamp out of range in {commit_oid}"))?,
            tz_offset: committer_time.offset.div_euclid(60),
        }),
    };

    Ok(ParsedGitCommit {
        raw_commit_bytes,
        tree_oid,
        parent_ids,
        author,
        committer,
        message: commit_ref.message.to_string(),
        parsed_change_id,
        extra_tree_ids,
        conflict_labels,
        secure_sig,
    })
}

fn validate_conflict_labels(
    labels: &[String],
    num_trees: usize,
    context: &str,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let has_non_empty_label = labels.iter().any(|l| !l.is_empty());
    let unlabeled = labels.is_empty()
        || (labels.len() == 1 && labels[0].is_empty())
        || (labels.len() == num_trees && !has_non_empty_label);
    if !unlabeled && (num_trees == 1 || labels.len() != num_trees) {
        return Err(format!(
            "{context} conflict_labels must be empty or match root_tree_id length (got {} labels for {num_trees} trees)",
            labels.len()
        )
        .into());
    }
    if labels.iter().any(|l| l.contains('\n')) {
        return Err(format!("{context} conflict_labels must not contain newlines").into());
    }
    Ok(())
}

fn raw_signature_matches(sig: Option<&Signature>, raw: gix::actor::SignatureRef<'_>) -> bool {
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

/// Validates structural commit fields (`commit_id`, `change_id`, `root_tree_id`,
/// `conflict_labels`, `parent_commit_ids`, `secure_sig`) before walking parents, fetching trees,
/// or serializing a Commit Cloud commit into Git.
pub(super) fn validate_export_commit_structure(
    expected_id: &[u8],
    commit: &Commit,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    if expected_id.len() != COMMIT_ID_LENGTH {
        return Err(format!(
            "Exported commit ID must be {COMMIT_ID_LENGTH} bytes (got {} bytes)",
            expected_id.len()
        )
        .into());
    }
    let cid_hex = hex::encode(expected_id);
    if !commit.commit_id.is_empty() && commit.commit_id.as_slice() != expected_id {
        return Err(format!(
            "ReadCommit response commit_id {} did not match requested {cid_hex}",
            hex::encode(&commit.commit_id)
        )
        .into());
    }
    if commit.change_id.len() != CHANGE_ID_LENGTH {
        return Err(format!(
            "Exported commit {cid_hex} change_id must be {CHANGE_ID_LENGTH} bytes (got {} bytes)",
            commit.change_id.len()
        )
        .into());
    }
    let num_trees = commit.root_tree_id.len();
    if num_trees == 0 || num_trees.is_multiple_of(2) {
        return Err(format!(
            "Exported commit {cid_hex} root_tree_id must hold a non-empty odd number of trees (got {num_trees})"
        )
        .into());
    }
    for tree_id in &commit.root_tree_id {
        if tree_id.len() != TREE_ID_LENGTH {
            return Err(format!(
                "Exported commit {cid_hex} root_tree_id entry must be {TREE_ID_LENGTH} bytes (got {} bytes)",
                tree_id.len()
            )
            .into());
        }
    }
    validate_conflict_labels(
        &commit.conflict_labels,
        num_trees,
        &format!("Exported commit {cid_hex}"),
    )?;
    if commit.parent_commit_ids.is_empty() {
        return Err(format!("Exported commit {cid_hex} must have at least one parent").into());
    }
    let has_root_parent = commit
        .parent_commit_ids
        .iter()
        .any(|id| id.as_slice() == ROOT_COMMIT_ID_BYTES);
    if has_root_parent && commit.parent_commit_ids.len() != 1 {
        return Err(format!(
            "Exported commit {cid_hex} may only include ROOT_COMMIT_ID as its sole parent"
        )
        .into());
    }
    for parent_id in &commit.parent_commit_ids {
        if parent_id.len() != COMMIT_ID_LENGTH {
            return Err(format!(
                "Exported commit {cid_hex} parent_commit_ids entry must be {COMMIT_ID_LENGTH} bytes (got {} bytes)",
                parent_id.len()
            )
            .into());
        }
    }
    if let Some(sig) = &commit.secure_sig
        && sig.sig.is_empty()
    {
        return Err(format!("Exported commit {cid_hex} secure_sig.sig must not be empty").into());
    }
    Ok(())
}

/// Verifies non-empty `commit.raw_git_commit` against `expected_id`, `commit.parent_commit_ids`,
/// `commit.root_tree_id` (`tree` / `jj:trees`), `description`, `author`, `committer`, `change_id`,
/// `conflict_labels`, and `secure_sig` before exporting its tree or writing the commit object.
pub(super) fn validate_raw_git_commit(
    expected_id: &[u8],
    commit: &Commit,
) -> Result<Option<Vec<u8>>, Box<dyn std::error::Error + Send + Sync>> {
    if commit.raw_git_commit.is_empty() {
        return Ok(None);
    }
    let cid_hex = hex::encode(expected_id);
    let digest = gix::objs::compute_hash(
        gix::hash::Kind::Sha1,
        gix::objs::Kind::Commit,
        &commit.raw_git_commit,
    )?;
    if digest.as_bytes() != expected_id {
        return Err(format!(
            "Exported Git commit raw_git_commit SHA-1 mismatch for {cid_hex}: computed {digest}"
        )
        .into());
    }
    let parsed = gix::objs::CommitRef::from_bytes(&commit.raw_git_commit, gix::hash::Kind::Sha1)
        .map_err(|e| format!("Failed to parse raw_git_commit for {cid_hex}: {e}"))?;
    let structured_parents: Vec<&[u8]> = commit
        .parent_commit_ids
        .iter()
        .map(Vec::as_slice)
        .filter(|id| *id != ROOT_COMMIT_ID_BYTES)
        .collect();
    let raw_parents: Vec<gix::hash::ObjectId> = parsed.parents().collect();
    if raw_parents.len() != structured_parents.len()
        || raw_parents
            .iter()
            .zip(structured_parents.iter())
            .any(|(rp, sp)| rp.as_bytes() != *sp)
    {
        return Err(format!(
            "Exported Git commit raw_git_commit parents do not match commit.parent_commit_ids for {cid_hex}"
        )
        .into());
    }
    match parse_jj_trees_header(&parsed)
        .map_err(|e| format!("Invalid raw_git_commit for {cid_hex}: {e}"))?
    {
        Some(raw_trees) => {
            if raw_trees.len() != commit.root_tree_id.len()
                || raw_trees
                    .iter()
                    .zip(commit.root_tree_id.iter())
                    .any(|(rt, st)| rt.as_bytes() != st.as_slice())
            {
                return Err(format!(
                    "Exported Git commit raw_git_commit jj:trees does not match commit.root_tree_id for {cid_hex}"
                )
                .into());
            }
        }
        None => {
            if commit.root_tree_id.len() != 1
                || commit.root_tree_id[0].as_slice() != parsed.tree().as_bytes()
            {
                return Err(format!(
                    "Exported Git commit raw_git_commit tree header does not match commit.root_tree_id for {cid_hex}"
                )
                .into());
            }
        }
    }
    if commit.description != String::from_utf8_lossy(parsed.message) {
        return Err(format!(
            "Exported Git commit raw_git_commit message does not match commit.description for {cid_hex}"
        )
        .into());
    }
    let raw_author = parsed
        .author()
        .map_err(|e| format!("Invalid raw_git_commit author for {cid_hex}: {e}"))?;
    if !raw_signature_matches(commit.author.as_ref(), raw_author) {
        return Err(format!(
            "Exported Git commit raw_git_commit author does not match commit.author for {cid_hex}"
        )
        .into());
    }
    let raw_committer = parsed
        .committer()
        .map_err(|e| format!("Invalid raw_git_commit committer for {cid_hex}: {e}"))?;
    if !raw_signature_matches(commit.committer.as_ref(), raw_committer) {
        return Err(format!(
            "Exported Git commit raw_git_commit committer does not match commit.committer for {cid_hex}"
        )
        .into());
    }

    let expected_change_id = parsed
        .extra_headers()
        .find("change-id")
        .and_then(|v| decode_change_id_header(v.as_ref()))
        .unwrap_or_else(|| {
            use jj_lib::object_id::ObjectId as _;
            let jj_cid = jj_lib::backend::CommitId::from_bytes(expected_id);
            jj_lib::git_backend::synthetic_change_id_from_git_commit_id(&jj_cid)
                .as_bytes()
                .to_vec()
        });
    if commit.change_id != expected_change_id {
        return Err(format!(
            "Exported Git commit raw_git_commit change-id does not match commit.change_id for {cid_hex}"
        )
        .into());
    }

    let normalized_proto_labels: &[String] =
        if commit.conflict_labels.iter().all(|label| label.is_empty()) {
            &[]
        } else {
            &commit.conflict_labels
        };
    match parsed.extra_headers().find("jj:conflict-labels") {
        Some(value) => {
            let mut labels: Vec<&str> = std::str::from_utf8(value.as_ref())
                .map_err(|_| {
                    format!(
                        "Exported Git commit raw_git_commit jj:conflict-labels is not UTF-8 for {cid_hex}"
                    )
                })?
                .split_terminator('\n')
                .collect();
            if labels.iter().all(|label| label.is_empty()) {
                let valid_empty_len = labels.is_empty()
                    || labels.len() == 1
                    || labels.len() == commit.root_tree_id.len();
                if !valid_empty_len {
                    return Err(format!(
                        "Exported Git commit raw_git_commit jj:conflict-labels has invalid length ({} labels for {} trees) for {cid_hex}",
                        labels.len(),
                        commit.root_tree_id.len()
                    )
                    .into());
                }
                labels.clear();
            }
            if labels.len() != normalized_proto_labels.len()
                || labels
                    .iter()
                    .zip(normalized_proto_labels.iter())
                    .any(|(rl, pl)| *rl != pl.as_str())
            {
                return Err(format!(
                    "Exported Git commit raw_git_commit jj:conflict-labels does not match commit.conflict_labels for {cid_hex}"
                )
                .into());
            }
        }
        None if !normalized_proto_labels.is_empty() => {
            return Err(format!(
                "Exported Git commit commit.conflict_labels lists labels absent from raw_git_commit for {cid_hex}"
            )
            .into());
        }
        None => {}
    }

    let raw_sig =
        gix::objs::CommitRefIter::signature(&commit.raw_git_commit, gix::hash::Kind::Sha1)
            .map_err(|e| format!("Invalid raw_git_commit gpgsig header for {cid_hex}: {e}"))?;
    match (raw_sig, commit.secure_sig.as_ref()) {
        (Some((raw_sig_bytes, raw_signed_data)), Some(sig)) => {
            if raw_sig_bytes.is_empty() {
                return Err(format!(
                    "Exported Git commit raw_git_commit has empty gpgsig header for {cid_hex}"
                )
                .into());
            }
            if raw_sig_bytes.as_ref() != sig.sig.as_slice() {
                return Err(format!(
                    "Exported Git commit raw_git_commit gpgsig does not match commit.secure_sig.sig for {cid_hex}"
                )
                .into());
            }
            if !sig.data.is_empty() {
                let canonical_data = Vec::from(raw_signed_data.to_bstring());
                if sig.data != canonical_data {
                    return Err(format!(
                        "Exported Git commit secure_sig.data does not match raw_git_commit payload for {cid_hex}"
                    )
                    .into());
                }
            }
        }
        (None, None) => {}
        (Some(_), None) => {
            return Err(format!(
                "Exported Git commit raw_git_commit has gpgsig header but commit.secure_sig is missing for {cid_hex}"
            )
            .into());
        }
        (None, Some(_)) => {
            return Err(format!(
                "Exported Git commit commit.secure_sig is set but raw_git_commit lacks gpgsig header for {cid_hex}"
            )
            .into());
        }
    }

    Ok(Some(parsed.tree().as_bytes().to_vec()))
}

pub(super) struct VerifiedExportCommit {
    pub(super) git_commit_bytes: Vec<u8>,
    pub(super) raw_tree_id: Option<Vec<u8>>,
}

/// Validates and resolves the exact Git commit bytes (both `raw_git_commit` and synthesized
/// non-raw commits) and verifies their SHA-1 against `expected_id` before any target repository
/// initialization or object writes.
pub(super) fn prepare_export_commit(
    expected_id: &[u8],
    commit: &Commit,
) -> Result<VerifiedExportCommit, Box<dyn std::error::Error + Send + Sync>> {
    validate_export_commit_structure(expected_id, commit)?;
    if !commit.raw_git_commit.is_empty() {
        let raw_tree_id = validate_raw_git_commit(expected_id, commit)?;
        return Ok(VerifiedExportCommit {
            git_commit_bytes: commit.raw_git_commit.clone(),
            raw_tree_id,
        });
    }

    let with_change_id = serialize_proto_commit_to_git(commit, true)?;
    let hash_with = gix::objs::compute_hash(
        gix::hash::Kind::Sha1,
        gix::objs::Kind::Commit,
        &with_change_id,
    )?;
    let git_commit_bytes = if hash_with.as_bytes() == expected_id {
        with_change_id
    } else {
        let without_change_id = serialize_proto_commit_to_git(commit, false)?;
        let hash_without = gix::objs::compute_hash(
            gix::hash::Kind::Sha1,
            gix::objs::Kind::Commit,
            &without_change_id,
        )?;
        if hash_without.as_bytes() == expected_id {
            without_change_id
        } else {
            return Err(format!(
                "Exported Git commit SHA-1 mismatch for {}: neither with-change-id ({hash_with}) nor native ({hash_without}) matched",
                hex::encode(expected_id)
            )
            .into());
        }
    };
    Ok(VerifiedExportCommit {
        git_commit_bytes,
        raw_tree_id: None,
    })
}

pub(super) fn write_verified_commit(
    thread_safe_repo: &gix::ThreadSafeRepository,
    expected_id: &[u8],
    git_commit_bytes: &[u8],
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let git_repo = thread_safe_repo.to_thread_local();
    let written = git_repo
        .objects
        .write_buf(gix::objs::Kind::Commit, git_commit_bytes)?;
    if written.as_bytes() != expected_id {
        return Err(format!(
            "Exported Git commit SHA-1 mismatch: expected {}, wrote {written}",
            hex::encode(expected_id)
        )
        .into());
    }
    Ok(())
}

#[cfg(test)]
pub(super) fn export_single_commit(
    thread_safe_repo: &gix::ThreadSafeRepository,
    expected_id: &[u8],
    commit: &Commit,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let verified = prepare_export_commit(expected_id, commit)?;
    write_verified_commit(thread_safe_repo, expected_id, &verified.git_commit_bytes)
}

pub(super) fn serialize_proto_commit_to_git(
    commit: &Commit,
    include_change_id: bool,
) -> Result<Vec<u8>, Box<dyn std::error::Error + Send + Sync>> {
    if commit.change_id.len() != CHANGE_ID_LENGTH {
        return Err(format!(
            "commit.change_id must be {CHANGE_ID_LENGTH} bytes (got {} bytes)",
            commit.change_id.len()
        )
        .into());
    }
    let num_trees = commit.root_tree_id.len();
    if num_trees == 0 || num_trees.is_multiple_of(2) {
        return Err(format!(
            "commit.root_tree_id must hold a non-empty odd number of trees (got {num_trees})"
        )
        .into());
    }
    let tree_ids: Vec<gix::hash::ObjectId> = commit
        .root_tree_id
        .iter()
        .map(|id| {
            if id.len() != TREE_ID_LENGTH {
                return Err(format!(
                    "commit.root_tree_id entry must be {TREE_ID_LENGTH} bytes (got {} bytes)",
                    id.len()
                )
                .into());
            }
            gix::hash::ObjectId::try_from(id.as_slice())
                .map_err(|e| format!("invalid commit.root_tree_id entry: {e}").into())
        })
        .collect::<Result<_, Box<dyn std::error::Error + Send + Sync>>>()?;
    let tree_id = tree_ids[0];
    validate_conflict_labels(&commit.conflict_labels, num_trees, "commit")?;

    if commit.parent_commit_ids.is_empty() {
        return Err("commit.parent_commit_ids must not be empty".into());
    }
    let has_root_parent = commit
        .parent_commit_ids
        .iter()
        .any(|id| id.as_slice() == ROOT_COMMIT_ID_BYTES);
    if has_root_parent && commit.parent_commit_ids.len() != 1 {
        return Err("ROOT_COMMIT_ID may only appear as the sole entry in parent_commit_ids".into());
    }
    let mut parents = Vec::with_capacity(commit.parent_commit_ids.len());
    for id in &commit.parent_commit_ids {
        if id.len() != COMMIT_ID_LENGTH {
            return Err(format!(
                "commit.parent_commit_ids entry must be {COMMIT_ID_LENGTH} bytes (got {} bytes)",
                id.len()
            )
            .into());
        }
        if id.as_slice() == ROOT_COMMIT_ID_BYTES {
            continue;
        }
        parents.push(
            gix::hash::ObjectId::try_from(id.as_slice())
                .map_err(|e| format!("invalid commit.parent_commit_ids entry: {e}"))?,
        );
    }

    let mut extra_headers = Vec::new();
    if tree_ids.len() > 1
        && commit.conflict_labels.len() > 1
        && commit.conflict_labels.iter().any(|l| !l.is_empty())
    {
        let mut joined = commit.conflict_labels.join("\n");
        joined.push('\n');
        extra_headers.push(("jj:conflict-labels".into(), joined.into()));
    }
    if tree_ids.len() > 1 {
        let trees_hex: Vec<String> = tree_ids.iter().map(ToString::to_string).collect();
        extra_headers.push(("jj:trees".into(), trees_hex.join(" ").into()));
    }
    if include_change_id && !commit.change_id.is_empty() {
        extra_headers.push((
            "change-id".into(),
            encode_reverse_hex(&commit.change_id).into(),
        ));
    }
    if let Some(sig) = &commit.secure_sig
        && !sig.sig.is_empty()
    {
        extra_headers.push(("gpgsig".into(), sig.sig.clone().into()));
    }

    let gix_commit = gix::objs::Commit {
        tree: tree_id,
        parents: parents.into(),
        author: signature_proto_to_gix(commit.author.as_ref())?,
        committer: signature_proto_to_gix(commit.committer.as_ref())?,
        encoding: None,
        extra_headers,
        message: commit.description.as_bytes().into(),
    };

    let mut buf = Vec::new();
    gix_commit.write_to(&mut buf)?;
    Ok(buf)
}

fn signature_proto_to_gix(
    sig: Option<&Signature>,
) -> Result<gix::actor::Signature, Box<dyn std::error::Error + Send + Sync>> {
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
    let (secs, offset_mins) = sig.and_then(|s| s.timestamp.as_ref()).map_or((0, 0), |t| {
        (t.millis_since_epoch.div_euclid(1000), t.tz_offset)
    });
    let offset_secs = offset_mins
        .checked_mul(60)
        .ok_or_else(|| format!("signature timezone offset {offset_mins} overflows seconds"))?;
    if offset_secs.unsigned_abs() / 3_600 > 99 {
        return Err(format!(
            "signature timezone offset {offset_mins} minutes exceeds Git +-9959 limit"
        )
        .into());
    }

    Ok(gix::actor::Signature {
        name: name.into(),
        email: email.into(),
        time: gix::date::Time::new(secs, offset_secs),
    })
}

pub(super) fn decode_change_id_header(raw: &[u8]) -> Option<Vec<u8>> {
    decode_reverse_hex(raw).filter(|id| id.len() == CHANGE_ID_LENGTH)
}
