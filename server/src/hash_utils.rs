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

//! Deterministic content-addressed object hashing and digest verification for jj-commit-cloud.
//!
//! # Authority and Crash-Safety Ordering
//! RPC handlers validate objects and compute canonical digests before persisting to
//! [`crate::store::Store`]. Commits, trees, blobs, symlinks, and gitlinks use 20-byte
//! Git SHA-1 digests via `gix` matching `jj_lib`'s Git serialization (conflicted commits name
//! their first tree term in `tree`, so their ID differs from `GitBackend`). Views and
//! operations use 16-byte truncated SHA-1 digests over length-prefixed, key-sorted bytes.
//!
//! # Concurrency and Merge Invariants
//! Map fields and tree entries are sorted before hashing so concurrent writers with identical
//! logical state converge on the same ID. Conflicted `Merge<TreeId>` commits encode all sides
//! in `jj:trees` and `jj:conflict-labels`. Malformed IDs, names, or counts are rejected
//! before hashing.
//!
//! # Cross-Process and RPC Boundaries
//! `WriteCommit` callers may omit `commit.commit_id` or supply an explicit digest (plus
//! optional `raw_git_commit`) when importing Git history; [`verify_or_compute_git_commit_hash`]
//! rejects mismatched digests or disagreeing headers and derives `secure_sig.data` server-side.

const EMPTY_STRING_PLACEHOLDER: &str = "JJ_EMPTY_STRING";
const REVERSE_HEX_CHARS: &[u8; 16] = b"zyxwvutsrqponmlk";
const FORWARD_HEX_CHARS: &[u8; 16] = b"0123456789abcdef";
/// Byte length of the Git SHA-1 IDs of commits, trees, blobs, symlinks, and gitlinks.
const SHA1_ID_LENGTH: usize = 20;

/// Converts a client-supplied object ID into a Git SHA-1 ID, rejecting any other length.
fn sha1_object_id(id: &[u8], field: &str) -> Result<gix::hash::ObjectId, String> {
    if id.len() != SHA1_ID_LENGTH {
        return Err(format!(
            "{field} must be {SHA1_ID_LENGTH} bytes (got {} bytes)",
            id.len()
        ));
    }
    gix::hash::ObjectId::try_from(id).map_err(|e| format!("invalid {field}: {e}"))
}

fn append_length_prefixed_bytes(buf: &mut Vec<u8>, bytes: &[u8]) {
    buf.extend_from_slice(&(bytes.len() as u64).to_le_bytes());
    buf.extend_from_slice(bytes);
}

fn append_length_prefixed_str(buf: &mut Vec<u8>, s: &str) {
    append_length_prefixed_bytes(buf, s.as_bytes());
}

pub fn encode_reverse_hex(data: &[u8]) -> String {
    let encoded: Vec<u8> = data
        .iter()
        .flat_map(|b| {
            [
                REVERSE_HEX_CHARS[usize::from(b >> 4)],
                REVERSE_HEX_CHARS[usize::from(b & 0x0f)],
            ]
        })
        .collect();
    String::from_utf8(encoded).expect("ASCII z-k digits must be valid UTF-8")
}

fn encode_forward_hex(data: &[u8]) -> String {
    let encoded: Vec<u8> = data
        .iter()
        .flat_map(|b| {
            [
                FORWARD_HEX_CHARS[usize::from(b >> 4)],
                FORWARD_HEX_CHARS[usize::from(b & 0x0f)],
            ]
        })
        .collect();
    String::from_utf8(encoded).expect("ASCII hex digits must be valid UTF-8")
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
    let (secs, offset_mins) = sig.and_then(|s| s.timestamp.as_ref()).map_or((0, 0), |t| {
        (t.millis_since_epoch.div_euclid(1000), t.tz_offset)
    });
    let offset_secs = offset_mins
        .checked_mul(60)
        .ok_or_else(|| format!("signature tz_offset {offset_mins} is out of range"))?;
    if offset_secs.unsigned_abs() / 3_600 > 99 {
        return Err(format!(
            "signature tz_offset {offset_mins} exceeds Git +-9959 limit"
        ));
    }

    Ok(gix::actor::Signature {
        name: name.into(),
        email: email.into(),
        time: gix::date::Time::new(secs, offset_secs),
    })
}

/// Serializes a `Commit` to canonical Git commit object bytes (`include_sig = false`
/// formats the unsigned payload passed to cryptographic commit signers).
///
/// Fails when a tree or parent ID is not a 20-byte SHA-1 ID, or when Git cannot represent a
/// field, such as `<`, `>`, or a newline in a signature name or email.
pub fn serialize_canonical_git_commit(
    commit: &cc_common::backend::Commit,
    include_sig: bool,
) -> Result<Vec<u8>, String> {
    use gix::objs::WriteTo;

    let tree_ids = commit
        .root_tree_id
        .iter()
        .map(|id| sha1_object_id(id, "root_tree_id"))
        .collect::<Result<Vec<_>, _>>()?;
    let tree_id = *tree_ids
        .first()
        .ok_or("commit must have at least one root tree")?;

    // Exclude jj's virtual root commit ID because Git root commits have zero parents.
    let parents: Vec<gix::hash::ObjectId> = commit
        .parent_commit_ids
        .iter()
        .filter(|id| id.as_slice() != cc_common::ROOT_COMMIT_ID_BYTES)
        .map(|id| sha1_object_id(id, "parent_commit_ids"))
        .collect::<Result<_, _>>()?;

    let mut extra_headers = Vec::new();
    if commit.root_tree_id.len() > 1
        && commit.conflict_labels.len() > 1
        && commit.conflict_labels.iter().any(|l| !l.is_empty())
    {
        let mut joined = commit.conflict_labels.join("\n");
        joined.push('\n');
        extra_headers.push(("jj:conflict-labels".into(), joined.into()));
    }
    if commit.root_tree_id.len() > 1 {
        let trees_hex: Vec<String> = commit
            .root_tree_id
            .iter()
            .map(|id| encode_forward_hex(id))
            .collect();
        extra_headers.push(("jj:trees".into(), trees_hex.join(" ").into()));
    }
    if !commit.change_id.is_empty() {
        let rev_hex = encode_reverse_hex(&commit.change_id);
        extra_headers.push(("change-id".into(), rev_hex.into()));
    }
    if include_sig
        && let Some(secure_sig) = &commit.secure_sig
        && !secure_sig.sig.is_empty()
    {
        extra_headers.push(("gpgsig".into(), secure_sig.sig.clone().into()));
    }

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
        .map_err(|e| format!("commit cannot be serialized as a Git object: {e}"))?;
    Ok(buf)
}

/// Computes a 20-byte Git commit SHA-1 digest over [`serialize_canonical_git_commit`]
/// (or over `raw_git_commit` when preserved from a native Git import).
pub fn compute_git_commit_hash(commit: &cc_common::backend::Commit) -> Result<Vec<u8>, String> {
    if !commit.raw_git_commit.is_empty() {
        let hash = gix::objs::compute_hash(
            gix::hash::Kind::Sha1,
            gix::objs::Kind::Commit,
            &commit.raw_git_commit,
        )
        .map_err(|e| format!("SHA-1 digest error: {e}"))?;
        return Ok(hash.as_bytes().to_vec());
    }
    let buf = serialize_canonical_git_commit(commit, true)?;
    let hash = gix::objs::compute_hash(gix::hash::Kind::Sha1, gix::objs::Kind::Commit, &buf)
        .map_err(|e| format!("SHA-1 digest error: {e}"))?;
    Ok(hash.as_bytes().to_vec())
}

/// Validates a client-supplied commit, computes its canonical 20-byte Git commit SHA-1
/// digest, and checks any client-supplied `commit_id` and `raw_git_commit` against the
/// structured `Commit` fields.
///
/// On success, `commit.secure_sig.data` holds the payload that `secure_sig.sig` signs, as
/// derived on the server (see [`derive_secure_sig_data`]).
pub fn verify_or_compute_git_commit_hash(
    commit: &mut cc_common::backend::Commit,
) -> Result<Vec<u8>, String> {
    if !commit.commit_id.is_empty() && commit.commit_id.len() != cc_common::COMMIT_ID_LENGTH {
        return Err(format!(
            "commit_id must be empty or {} bytes (got {} bytes)",
            cc_common::COMMIT_ID_LENGTH,
            commit.commit_id.len()
        ));
    }
    validate_and_canonicalize_commit_fields(commit)?;

    let computed = if !commit.raw_git_commit.is_empty() {
        let computed = gix::objs::compute_hash(
            gix::hash::Kind::Sha1,
            gix::objs::Kind::Commit,
            &commit.raw_git_commit,
        )
        .map_err(|e| format!("SHA-1 digest error: {e}"))?
        .as_bytes()
        .to_vec();

        let parsed =
            gix::objs::CommitRef::from_bytes(&commit.raw_git_commit, gix::hash::Kind::Sha1)
                .map_err(|e| format!("invalid raw_git_commit bytes: {e}"))?;

        sha1_object_id(parsed.tree().as_bytes(), "raw_git_commit tree header")?;
        if parsed.extra_headers().find("jj:trees").is_none() {
            let expected_first_tree = commit
                .root_tree_id
                .first()
                .map(Vec::as_slice)
                .unwrap_or(&cc_common::EMPTY_TREE_ID_BYTES);
            if parsed.tree().as_bytes() != expected_first_tree {
                return Err(format!(
                    "raw_git_commit tree header {} does not match commit.root_tree_id {}",
                    parsed.tree(),
                    hex::encode(expected_first_tree)
                ));
            }
        }

        let structured_parents: Vec<&[u8]> = commit
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
            return Err(
                "raw_git_commit parent headers do not match commit.parent_commit_ids".to_string(),
            );
        }
        verify_raw_git_commit_metadata(commit, &parsed, &computed)?;
        computed
    } else {
        compute_git_commit_hash(commit)?
    };

    if !commit.commit_id.is_empty() && commit.commit_id != computed {
        return Err(format!(
            "client commit_id {} does not match canonical Git SHA-1 {}",
            hex::encode(&commit.commit_id),
            hex::encode(&computed)
        ));
    }

    derive_secure_sig_data(commit)?;
    Ok(computed)
}

/// Validates structural `Commit` fields and canonicalizes all-empty `conflict_labels`
/// (including legacy `[""]` and per-tree all-empty labels) to `[]`.
pub(crate) fn validate_and_canonicalize_commit_fields(
    commit: &mut cc_common::backend::Commit,
) -> Result<(), String> {
    validate_commit_fields(commit)?;
    if commit.conflict_labels.iter().all(|label| label.is_empty()) {
        commit.conflict_labels.clear();
    }
    Ok(())
}

/// Rejects commits that jj clients cannot load or that the Git serialization would not
/// cover exactly.
fn validate_commit_fields(commit: &cc_common::backend::Commit) -> Result<(), String> {
    if commit.change_id.len() != cc_common::CHANGE_ID_LENGTH {
        return Err(format!(
            "change_id must be {} bytes (got {} bytes)",
            cc_common::CHANGE_ID_LENGTH,
            commit.change_id.len()
        ));
    }
    // `Merge<TreeId>` holds one tree, or 2N+1 terms for a conflict.
    let num_trees = commit.root_tree_id.len();
    if num_trees.is_multiple_of(2) {
        return Err(format!(
            "root_tree_id must hold an odd number of trees (got {num_trees})"
        ));
    }
    for tree_id in &commit.root_tree_id {
        sha1_object_id(tree_id, "root_tree_id")?;
    }
    // Canonical callers omit `conflict_labels` when unlabeled; accept legacy `[""]`
    // from older PoC clients (canonicalized to `[]` by `verify_or_compute_git_commit_hash`)
    // as well as one label per conflict tree.
    let labels = &commit.conflict_labels;
    let legacy_unlabeled = labels.len() == 1 && labels[0].is_empty();
    let valid_conflict_labels = num_trees > 1 && labels.len() == num_trees;
    if !labels.is_empty() && !legacy_unlabeled && !valid_conflict_labels {
        return Err(format!(
            "conflict_labels must be empty or hold one label per tree (got {} labels for {num_trees} trees)",
            labels.len()
        ));
    }
    // Labels are joined with newlines in the `jj:conflict-labels` header.
    if labels.iter().any(|label| label.contains('\n')) {
        return Err("conflict_labels must not contain newlines".to_string());
    }
    if commit.parent_commit_ids.is_empty() {
        return Err("commit must have at least one parent".to_string());
    }
    let has_root_parent = commit
        .parent_commit_ids
        .iter()
        .any(|id| id.as_slice() == cc_common::ROOT_COMMIT_ID_BYTES);
    if has_root_parent && commit.parent_commit_ids.len() != 1 {
        return Err(
            "The Commit Cloud backend does not support creating merge commits with the root commit as one of the parents (ROOT_COMMIT_ID may only appear as the sole entry in parent_commit_ids)"
                .to_string(),
        );
    }
    for parent_id in &commit.parent_commit_ids {
        sha1_object_id(parent_id, "parent_commit_ids")?;
    }
    for predecessor_id in &commit.predecessors {
        sha1_object_id(predecessor_id, "predecessors")?;
    }
    validate_external_ids(&commit.external_ids)?;
    Ok(())
}

fn validate_external_ids(
    external_ids: &std::collections::HashMap<String, Vec<u8>>,
) -> Result<(), String> {
    for (key, value) in external_ids {
        if key.is_empty()
            || key.len() > 128
            || key == "."
            || key == ".."
            || !key
                .bytes()
                .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'-' | b'_' | b'.'))
        {
            return Err(format!("invalid external_ids key {key:?}"));
        }
        if value.is_empty() || value.len() > 64 {
            return Err(format!(
                "external_ids[{key:?}] must be 1..=64 bytes (got {} bytes)",
                value.len()
            ));
        }
        match key.as_str() {
            "git-sha1" if value.len() != SHA1_ID_LENGTH => {
                return Err(format!(
                    "external_ids['git-sha1'] must be {SHA1_ID_LENGTH} bytes (got {} bytes)",
                    value.len()
                ));
            }
            "git-sha256" if value.len() != 32 => {
                return Err(format!(
                    "external_ids['git-sha256'] must be 32 bytes (got {} bytes)",
                    value.len()
                ));
            }
            _ => {}
        }
    }
    Ok(())
}

/// Checks the fields that jj reads from a commit against the `raw_git_commit` message and
/// headers, parsing them the way `jj_lib`'s `GitBackend` does. The caller has already
/// checked the `tree` and `parent` headers.
fn verify_raw_git_commit_metadata(
    commit: &cc_common::backend::Commit,
    parsed: &gix::objs::CommitRef<'_>,
    commit_id: &[u8],
) -> Result<(), String> {
    if commit.description != String::from_utf8_lossy(parsed.message) {
        return Err("raw_git_commit message does not match commit.description".to_string());
    }
    let raw_author = parsed
        .author()
        .map_err(|e| format!("invalid raw_git_commit author header: {e}"))?;
    if !raw_signature_matches(commit.author.as_ref(), raw_author) {
        return Err("raw_git_commit author does not match commit.author".to_string());
    }
    let raw_committer = parsed
        .committer()
        .map_err(|e| format!("invalid raw_git_commit committer header: {e}"))?;
    if !raw_signature_matches(commit.committer.as_ref(), raw_committer) {
        return Err("raw_git_commit committer does not match commit.committer".to_string());
    }

    let expected_change_id = parsed
        .extra_headers()
        .find("change-id")
        .and_then(jj_lib::hex_util::decode_reverse_hex)
        .filter(|id| id.len() == cc_common::CHANGE_ID_LENGTH)
        .unwrap_or_else(|| {
            use jj_lib::object_id::ObjectId as _;
            let jj_commit_id = jj_lib::backend::CommitId::from_bytes(commit_id);
            jj_lib::git_backend::synthetic_change_id_from_git_commit_id(&jj_commit_id)
                .as_bytes()
                .to_vec()
        });
    if commit.change_id != expected_change_id {
        return Err(
            "raw_git_commit change-id does not match commit.change_id (expected header or synthetic Git change ID)"
                .to_string(),
        );
    }

    match parsed.extra_headers().find("jj:trees") {
        Some(value) => {
            let tree_ids = value
                .split(|b| *b == b' ')
                .map(|hex| {
                    jj_lib::hex_util::decode_hex(hex).filter(|id| id.len() == SHA1_ID_LENGTH)
                })
                .collect::<Option<Vec<_>>>()
                .filter(|ids| ids.len() > 1 && ids.len() % 2 == 1)
                .ok_or("raw_git_commit has an invalid jj:trees header")?;
            if tree_ids != commit.root_tree_id {
                return Err(
                    "raw_git_commit jj:trees header does not match commit.root_tree_id".to_string(),
                );
            }
        }
        None if commit.root_tree_id.len() != 1 => {
            return Err(
                "commit.root_tree_id lists conflict terms that raw_git_commit does not record"
                    .to_string(),
            );
        }
        None => {}
    }

    match parsed.extra_headers().find("jj:conflict-labels") {
        Some(value) => {
            let mut labels: Vec<&str> = std::str::from_utf8(value)
                .map_err(|_| "raw_git_commit jj:conflict-labels header is not UTF-8")?
                .split_terminator('\n')
                .collect();
            if labels.iter().all(|label| label.is_empty()) {
                let valid_empty_len = labels.is_empty()
                    || labels.len() == 1
                    || labels.len() == commit.root_tree_id.len();
                if !valid_empty_len {
                    return Err(format!(
                        "raw_git_commit jj:conflict-labels header has invalid length ({} labels for {} trees)",
                        labels.len(),
                        commit.root_tree_id.len()
                    ));
                }
                labels.clear();
            }
            if labels != commit.conflict_labels {
                return Err(
                    "raw_git_commit jj:conflict-labels header does not match commit.conflict_labels"
                        .to_string(),
                );
            }
        }
        None if commit.conflict_labels.iter().any(|label| !label.is_empty()) => {
            return Err(
                "commit.conflict_labels lists labels that raw_git_commit does not record"
                    .to_string(),
            );
        }
        None => {}
    }
    Ok(())
}

/// Compares a structured signature with a raw Git author or committer line, accepting either
/// form of jj's `JJ_EMPTY_STRING` placeholder for empty names and emails.
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

/// Replaces `secure_sig.data` with the payload that `secure_sig.sig` signs, derived on the
/// server: `raw_git_commit` without its `gpgsig` header (as `jj_lib`'s `GitBackend` reads
/// it), or the canonical unsigned serialization. A non-empty client-supplied `data` must
/// already equal that payload, so a signature cannot be replayed onto another commit. A
/// `secure_sig` without a signature is dropped.
fn derive_secure_sig_data(commit: &mut cc_common::backend::Commit) -> Result<(), String> {
    use gix::bstr::ByteSlice as _;

    let client_sig = commit
        .secure_sig
        .as_ref()
        .map(|s| s.sig.as_slice())
        .filter(|sig| !sig.is_empty());
    let signed_data = if commit.raw_git_commit.is_empty() {
        match client_sig {
            Some(_) => Some(serialize_canonical_git_commit(commit, false)?),
            None => None,
        }
    } else {
        let raw_signature =
            gix::objs::CommitRefIter::signature(&commit.raw_git_commit, gix::hash::Kind::Sha1)
                .map_err(|e| format!("invalid raw_git_commit bytes: {e}"))?;
        let raw_sig = raw_signature.as_ref().map(|(sig, _)| sig.as_bytes());
        if raw_sig != client_sig {
            return Err(
                "secure_sig.sig does not match the raw_git_commit gpgsig header".to_string(),
            );
        }
        raw_signature.map(|(_, signed)| signed.to_bstring().into())
    };

    match signed_data {
        // `signed_data` is only set when the client supplied the matching signature.
        Some(data) => {
            if let Some(secure_sig) = commit.secure_sig.as_mut() {
                if !secure_sig.data.is_empty() && secure_sig.data != data {
                    return Err(
                        "secure_sig.data does not match the signed payload of this commit"
                            .to_string(),
                    );
                }
                secure_sig.data = data;
            }
        }
        None => commit.secure_sig = None,
    }
    Ok(())
}

/// Computes a 20-byte Git blob SHA-1 digest for file payloads and symlink target paths.
pub fn try_compute_git_blob_hash(content: &[u8]) -> Result<Vec<u8>, String> {
    let hash = gix::objs::compute_hash(gix::hash::Kind::Sha1, gix::objs::Kind::Blob, content)
        .map_err(|e| format!("SHA-1 digest computation error: {e}"))?;
    Ok(hash.as_bytes().to_vec())
}

/// Computes a 20-byte Git tree SHA-1 digest in canonical Git entry sort order.
///
/// Rejects entries that the digest could not cover exactly: missing or `ConflictId` values,
/// object IDs that are not 20 bytes, and names that are empty, `.`, `..`, duplicated, or
/// contain `/` or NUL.
pub fn compute_git_tree_hash(entries: &[cc_common::backend::TreeEntry]) -> Result<Vec<u8>, String> {
    use gix::objs::WriteTo;

    let mut names = std::collections::HashSet::with_capacity(entries.len());
    let mut gix_entries = Vec::with_capacity(entries.len());
    for entry in entries {
        let name = entry.name.as_str();
        if name.is_empty() || name == "." || name == ".." || name.contains(['/', '\0']) {
            return Err(format!("invalid tree entry name {name:?}"));
        }
        if !names.insert(name) {
            return Err(format!("duplicate tree entry name {name:?}"));
        }
        let (kind, id_bytes) = match entry.value.as_ref().and_then(|v| v.value.as_ref()) {
            Some(cc_common::backend::tree_value::Value::File(f)) => {
                let k = if f.executable {
                    gix::objs::tree::EntryKind::BlobExecutable
                } else {
                    gix::objs::tree::EntryKind::Blob
                };
                (k, &f.id[..])
            }
            Some(cc_common::backend::tree_value::Value::TreeId(id)) => {
                (gix::objs::tree::EntryKind::Tree, &id[..])
            }
            Some(cc_common::backend::tree_value::Value::SymlinkId(id)) => {
                (gix::objs::tree::EntryKind::Link, &id[..])
            }
            Some(cc_common::backend::tree_value::Value::GitSubmoduleId(id)) => {
                (gix::objs::tree::EntryKind::Commit, &id[..])
            }
            Some(cc_common::backend::tree_value::Value::ConflictId(_)) => {
                return Err(format!(
                    "tree entry {name:?} has an unsupported ConflictId value"
                ));
            }
            None => return Err(format!("tree entry {name:?} has no value")),
        };
        let oid = sha1_object_id(id_bytes, "tree entry object ID")?;
        gix_entries.push(gix::objs::tree::Entry {
            mode: kind.into(),
            filename: entry.name.as_bytes().into(),
            oid,
        });
    }

    gix_entries.sort_unstable();

    let gix_tree = gix::objs::Tree {
        entries: gix_entries,
    };
    let mut buf = Vec::new();
    gix_tree
        .write_to(&mut buf)
        .map_err(|e| format!("tree cannot be serialized: {e}"))?;

    let hash = gix::objs::compute_hash(gix::hash::Kind::Sha1, gix::objs::Kind::Tree, &buf)
        .map_err(|e| format!("SHA-1 digest error: {e}"))?;
    Ok(hash.as_bytes().to_vec())
}

/// Computes a deterministic 16-byte OperationId digest over length-prefixed, sorted fields.
pub fn hash_operation(op: &cc_common::op_store::Operation) -> Result<Vec<u8>, String> {
    let mut buf = Vec::new();
    append_length_prefixed_bytes(&mut buf, &op.view_id);
    buf.extend_from_slice(&(op.parents.len() as u64).to_le_bytes());
    for p in &op.parents {
        append_length_prefixed_bytes(&mut buf, p);
    }
    if let Some(meta) = &op.metadata {
        buf.push(1);
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
        let sorted_attrs: std::collections::BTreeMap<_, _> = meta.attributes.iter().collect();
        buf.extend_from_slice(&(sorted_attrs.len() as u64).to_le_bytes());
        for (k, v) in sorted_attrs {
            append_length_prefixed_str(&mut buf, k);
            append_length_prefixed_str(&mut buf, v);
        }
    } else {
        buf.push(0);
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
        .map_err(|e| format!("SHA-1 digest error: {e}"))?;
    Ok(hash.as_bytes()[..cc_common::OPERATION_ID_LENGTH].to_vec())
}

/// Computes a deterministic 16-byte ViewId digest over length-prefixed, sorted fields.
pub fn hash_view(view: &cc_common::op_store::View) -> Result<Vec<u8>, String> {
    let mut buf = Vec::new();
    let mut head_ids = view.head_ids.clone();
    head_ids.sort();
    head_ids.dedup();
    buf.extend_from_slice(&(head_ids.len() as u64).to_le_bytes());
    for head in &head_ids {
        append_length_prefixed_bytes(&mut buf, head);
    }

    let sorted_wc: std::collections::BTreeMap<_, _> = view.wc_commit_ids.iter().collect();
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

    let sorted_bookmarks: std::collections::BTreeMap<_, _> = view.local_bookmarks.iter().collect();
    buf.extend_from_slice(&(sorted_bookmarks.len() as u64).to_le_bytes());
    for (name, target) in sorted_bookmarks {
        append_length_prefixed_str(&mut buf, name);
        append_ref_target(&mut buf, target);
    }

    let sorted_remotes: std::collections::BTreeMap<_, _> = view.remote_bookmarks.iter().collect();
    buf.extend_from_slice(&(sorted_remotes.len() as u64).to_le_bytes());
    for (name, remote_ref) in sorted_remotes {
        append_length_prefixed_str(&mut buf, name);
        buf.push(remote_ref.is_tracked as u8);
        if let Some(target) = &remote_ref.target {
            buf.push(1);
            append_ref_target(&mut buf, target);
        } else {
            buf.push(0);
        }
    }

    let hash = gix::objs::compute_hash(gix::hash::Kind::Sha1, gix::objs::Kind::Blob, &buf)
        .map_err(|e| format!("SHA-1 digest error: {e}"))?;
    Ok(hash.as_bytes()[..cc_common::VIEW_ID_LENGTH].to_vec())
}

#[cfg(test)]
mod tests {
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
        let text =
            String::from_utf8(serialize_canonical_git_commit(&commit, false).unwrap()).unwrap();
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
        let hash = gix::objs::compute_hash(gix::hash::Kind::Sha1, gix::objs::Kind::Commit, raw)
            .expect("SHA-1 digest computation should succeed");
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
            commit_id: gix::objs::compute_hash(
                gix::hash::Kind::Sha1,
                gix::objs::Kind::Commit,
                &raw,
            )
            .expect("SHA-1 digest computation should succeed")
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

        for (bad_key, bad_val) in [
            ("", vec![1]),
            (".", vec![1]),
            ("..", vec![1]),
            ("bad/key", vec![1]),
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
    }
}
