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

//! Computing and checking [`Digest`]s.
//!
//! Which algorithm is native is still open (DEV-79). jj's own backend uses
//! BLAKE2b-512 for commit ids; adding it here is one enum variant and one
//! match arm.

use crate::error::{ErrorKind, StoreError, StoreResult};
use crate::ids::{Digest, HashAlgorithm};
use sha1::Sha1;
use sha2::{Digest as _, Sha256};

/// Hash `data` with `algorithm`.
pub fn compute(algorithm: HashAlgorithm, data: &[u8]) -> Digest {
    let bytes = match algorithm {
        HashAlgorithm::Sha256 => Sha256::digest(data).to_vec(),
        HashAlgorithm::Sha1 => Sha1::digest(data).to_vec(),
    };
    Digest { algorithm, bytes }
}

/// True if `data` hashes to `expected` with `expected.algorithm`.
pub fn verify(expected: &Digest, data: &[u8]) -> bool {
    compute(expected.algorithm, data) == *expected
}

impl Digest {
    /// Lower-case hex of the digest bytes.
    pub fn to_hex(&self) -> String {
        hex(&self.bytes)
    }

    /// Fails with `InvalidArgument` unless the digest is as long as its
    /// algorithm's output. Stores check every digest they are given to
    /// keep, so a malformed one never reaches storage.
    pub fn check_len(&self) -> StoreResult<()> {
        if self.bytes.len() != self.algorithm.digest_len() {
            return Err(StoreError::new(
                ErrorKind::InvalidArgument,
                format!(
                    "{} digest must be {} bytes, got {}",
                    self.algorithm.name(),
                    self.algorithm.digest_len(),
                    self.bytes.len()
                ),
            ));
        }
        Ok(())
    }
}

/// Lower-case hex of `bytes`.
pub fn hex(bytes: &[u8]) -> String {
    const DIGITS: &[u8; 16] = b"0123456789abcdef";
    let mut out = String::with_capacity(bytes.len() * 2);
    for b in bytes {
        out.push(char::from(DIGITS[usize::from(b >> 4)]));
        out.push(char::from(DIGITS[usize::from(b & 0x0f)]));
    }
    out
}

impl HashAlgorithm {
    /// Short stable name, suitable for paths and logs.
    pub fn name(self) -> &'static str {
        match self {
            HashAlgorithm::Sha256 => "sha256",
            HashAlgorithm::Sha1 => "sha1",
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sha256_of_empty_input() {
        let d = compute(HashAlgorithm::Sha256, b"");
        assert_eq!(
            d.to_hex(),
            "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
        );
        assert!(verify(&d, b""));
        assert!(!verify(&d, b"x"));
    }

    #[test]
    fn sha1_of_empty_input() {
        let d = compute(HashAlgorithm::Sha1, b"");
        assert_eq!(d.to_hex(), "da39a3ee5e6b4b0d3255bfef95601890afd80709");
    }
}
