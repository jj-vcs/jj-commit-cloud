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
//! `MetadataStore` or `BlobStore`. Commits, trees, blobs, symlinks, and gitlinks use 20-byte
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
            "ROOT_COMMIT_ID may only appear as the sole entry in parent_commit_ids".to_string(),
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
    const MAX_EXTERNAL_IDS: usize = 16;
    const MAX_EXTERNAL_ID_KEY_LEN: usize = 64;
    const MAX_ACTOR_EXTERNAL_ID_LEN: usize = 256;

    let non_actor_count = external_ids
        .keys()
        .filter(|k| !matches!(k.as_str(), "actor-sub" | "actor-uid"))
        .count();
    if non_actor_count > MAX_EXTERNAL_IDS {
        return Err(format!(
            "external_ids count ({non_actor_count}) exceeds maximum ({MAX_EXTERNAL_IDS})"
        ));
    }
    for (key, value) in external_ids {
        if key.is_empty()
            || key.len() > MAX_EXTERNAL_ID_KEY_LEN
            || key == "."
            || key == ".."
            || !key.bytes().all(|b| {
                b.is_ascii_lowercase() || b.is_ascii_digit() || matches!(b, b'-' | b'_' | b'.')
            })
        {
            return Err(format!(
                "invalid external_ids key {key:?} (must be 1..={MAX_EXTERNAL_ID_KEY_LEN} ASCII [a-z0-9._-] characters)"
            ));
        }
        match key.as_str() {
            "git-sha1" => {
                if value.len() != SHA1_ID_LENGTH {
                    return Err(format!(
                        "external_ids['git-sha1'] must be {SHA1_ID_LENGTH} bytes (got {} bytes)",
                        value.len()
                    ));
                }
            }
            "git-sha256" => {
                if value.len() != 32 {
                    return Err(format!(
                        "external_ids['git-sha256'] must be 32 bytes (got {} bytes)",
                        value.len()
                    ));
                }
            }
            "actor-sub" | "actor-uid" => {
                if value.is_empty() || value.len() > MAX_ACTOR_EXTERNAL_ID_LEN {
                    return Err(format!(
                        "external_ids[{key:?}] must be 1..={MAX_ACTOR_EXTERNAL_ID_LEN} bytes (got {} bytes)",
                        value.len()
                    ));
                }
                let text = std::str::from_utf8(value)
                    .map_err(|_| format!("external_ids[{key:?}] must be valid UTF-8"))?;
                if text.trim().is_empty() || text.bytes().any(|b| b.is_ascii_control()) {
                    return Err(format!(
                        "external_ids[{key:?}] must not be blank or contain ASCII control characters"
                    ));
                }
            }
            _ => {
                if value.is_empty() || value.len() > 64 {
                    return Err(format!(
                        "external_ids[{key:?}] must be 1..=64 bytes (got {} bytes)",
                        value.len()
                    ));
                }
            }
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
                if !f.copy_id.is_empty() {
                    return Err(format!(
                        "tree entry {name:?} has a non-empty copy_id, which the Git tree digest cannot bind"
                    ));
                }
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
        .map_err(|e| format!("SHA-1 digest computation error: {e}"))?;
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
        .map_err(|e| format!("SHA-1 digest computation error: {e}"))?;
    Ok(hash.as_bytes()[..cc_common::VIEW_ID_LENGTH].to_vec())
}

#[cfg(test)]
mod tests;
