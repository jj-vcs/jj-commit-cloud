#!/usr/bin/env bash
# Copyright 2026 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# Initializes an isolated per-actor Commit Cloud workspace inside a gVisor / Agent Substrate pod.
#
# Required environment variables:
#   JJ_CC_SERVER:     Commit Cloud gRPC endpoint (e.g. http://jj-cc-server:50051)
#   JJ_CC_PROJECT_ID: Shared Commit Cloud project_id
#   ATESPACE:         Agent Substrate atespace claim (scopes REPO_ID as <8-hex>_<atespace>_<actorName>)
#   ACTOR_NAME:       Agent Substrate actorName claim (must match repo_id suffix)
#   BASE_COMMIT:      Full 40-char Git SHA-1 base commit ID to index on startup
#
# Optional environment variables:
#   WORKSPACE_DIR:    Destination checkout directory (default: /workspace)
#   JJ_BIN:           Path to custom jj binary (default: jj)
#   JJ_CC_AUTH_TOKEN: Bearer JWT / ServiceAccount token forwarded by jj

set -euo pipefail

: "${JJ_CC_SERVER:?JJ_CC_SERVER must be set (e.g. http://jj-cc-server:50051)}"
: "${JJ_CC_PROJECT_ID:?JJ_CC_PROJECT_ID must be set}"
: "${ATESPACE:?ATESPACE must be set to a non-empty Agent Substrate atespace claim}"
: "${ACTOR_NAME:?ACTOR_NAME must be set}"
: "${BASE_COMMIT:?BASE_COMMIT must be set to a full 40-char commit ID}"

(
  export LC_ALL=C
  if [[ ! "$JJ_CC_PROJECT_ID" =~ ^[A-Za-z0-9._-]{1,128}$ ]] || [[ "$JJ_CC_PROJECT_ID" == "." ]] || [[ "$JJ_CC_PROJECT_ID" == ".." ]]; then
    echo "error: JJ_CC_PROJECT_ID must be 1-128 characters matching [A-Za-z0-9._-] (not '.' or '..')" >&2
    exit 1
  fi
  if [[ ! "$ATESPACE" =~ ^[A-Za-z0-9][A-Za-z0-9.-]*$ ]]; then
    echo "error: ATESPACE must start with an ASCII alphanumeric character and match ^[A-Za-z0-9][A-Za-z0-9.-]*$ (no '_' allowed)" >&2
    exit 1
  fi
  if [[ ! "$ACTOR_NAME" =~ ^[A-Za-z0-9][A-Za-z0-9.-]*$ ]]; then
    echo "error: ACTOR_NAME must start with an ASCII alphanumeric character and match ^[A-Za-z0-9][A-Za-z0-9.-]*$ (no '_' allowed)" >&2
    exit 1
  fi
  if [[ ! "$BASE_COMMIT" =~ ^[0-9a-fA-F]{40}$ ]]; then
    echo "error: BASE_COMMIT must be a 40-character hexadecimal Git commit ID" >&2
    exit 1
  fi
)

WORKSPACE_DIR="${WORKSPACE_DIR:-/workspace}"
JJ_BIN="${JJ_BIN:-jj}"

compute_sha256_prefix() {
  local input="$1"
  local raw=""
  if command -v sha256sum >/dev/null 2>&1; then
    raw="$(printf '%s' "$input" | sha256sum | cut -c1-8)"
  elif command -v shasum >/dev/null 2>&1; then
    raw="$(printf '%s' "$input" | shasum -a 256 | cut -c1-8)"
  elif command -v openssl >/dev/null 2>&1; then
    raw="$(printf '%s' "$input" | openssl dgst -sha256 | awk '{print substr($NF, 1, 8)}')"
  else
    echo "error: sha256sum, shasum, or openssl is required to compute REPO_ID prefix" >&2
    exit 1
  fi
  printf '%s' "$raw" | LC_ALL=C tr '[:upper:]' '[:lower:]'
}

HASH_PREFIX="$(compute_sha256_prefix "${JJ_CC_PROJECT_ID}:${ATESPACE}:${ACTOR_NAME}")"
export REPO_ID="${HASH_PREFIX}_${ATESPACE}_${ACTOR_NAME}"
(
  export LC_ALL=C
  if ! (( ${#REPO_ID} <= 128 )); then
    echo "error: REPO_ID '${REPO_ID}' is ${#REPO_ID} bytes, exceeding the 128-byte combined limit (<8-hex>_<atespace>_<actorName>)" >&2
    exit 1
  fi
)
export JJ_CC_SYNC_PROJECT_COMMITS=0
export JJ_CC_USE_DAEMON=0

parent_dir="$(dirname -- "$WORKSPACE_DIR")"
if [[ -n "$parent_dir" && "$parent_dir" != "." ]]; then
  mkdir -p -- "$parent_dir"
fi

"$JJ_BIN" cc init \
  --server="$JJ_CC_SERVER" \
  --project-id="$JJ_CC_PROJECT_ID" \
  --repo-id="$REPO_ID" \
  --working-copy-type=local \
  --index-commit="$BASE_COMMIT" \
  --create \
  -- "$WORKSPACE_DIR" >&2

if ! "$JJ_BIN" --repository="$WORKSPACE_DIR" cc daemon disable >&2; then
  echo "warning: unable to persist global jj cc daemon disable (continuing with JJ_CC_USE_DAEMON=0)" >&2
fi
"$JJ_BIN" --repository="$WORKSPACE_DIR" config set --repo commit-cloud.sync-project-commits false >&2
"$JJ_BIN" --repository="$WORKSPACE_DIR" new "$BASE_COMMIT" >&2

printf 'export REPO_ID=%q\nexport JJ_CC_SYNC_PROJECT_COMMITS=0\nexport JJ_CC_USE_DAEMON=0\n' "$REPO_ID"
