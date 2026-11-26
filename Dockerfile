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

# Multi-target container build for jj-cc-server, jj-cc-daemon, custom jj CLI, and xtask smoke runner.
# - Default target (`server`): minimal runtime image for Cloud Run, GKE, GCE, and Compose.
# - `--target cli`: runtime image with custom `jj`, `jj-cc-daemon`, and `xtask` for containerized workflows.
# - `--target bins`: scratch export stage for extracting compiled Linux binaries (`--output ./bin`).
FROM docker.io/library/rust:bookworm AS builder

WORKDIR /app
# Omit rust-toolchain.toml so the builder uses the preinstalled rust:bookworm
# toolchain without downloading extra rustfmt/clippy components via rustup.
COPY Cargo.toml Cargo.lock ./
COPY .cargo ./.cargo
COPY common/Cargo.toml ./common/Cargo.toml
COPY lib/Cargo.toml ./lib/Cargo.toml
COPY server/Cargo.toml ./server/Cargo.toml
COPY daemon/Cargo.toml ./daemon/Cargo.toml
COPY cli/Cargo.toml ./cli/Cargo.toml
COPY testutils/Cargo.toml ./testutils/Cargo.toml
COPY xtask/Cargo.toml ./xtask/Cargo.toml

# Pre-build and cache third-party Cargo dependencies before copying workspace sources.
RUN mkdir -p common/src lib/src server/src daemon/src cli/src testutils/src xtask/src \
    && printf 'fn main() {}\n' > common/build.rs \
    && touch common/src/lib.rs lib/src/lib.rs server/src/lib.rs testutils/src/lib.rs \
    && touch daemon/src/lib.rs \
    && printf 'fn main() {}\n' > daemon/src/main.rs \
    && printf 'fn main() {}\n' > server/src/main.rs \
    && printf 'fn main() {}\n' > cli/src/main.rs \
    && printf 'fn main() {}\n' > xtask/src/main.rs \
    && cargo build --locked --release \
        -p jj-commit-cloud-server --bin jj-cc-server \
        -p jj-commit-cloud-daemon --bin jj-cc-daemon \
        -p cli --bin jj \
        -p xtask --bin xtask \
    && rm -rf common/build.rs common/src lib/src server/src daemon/src cli/src testutils/src xtask/src \
        target/release/build/cc-common* \
        target/release/.fingerprint/cc-common* \
        target/release/.fingerprint/cc-lib* \
        target/release/.fingerprint/jj-commit-cloud* \
        target/release/.fingerprint/cli* \
        target/release/.fingerprint/xtask* \
        target/release/deps/libcc_* \
        target/release/deps/libjj_commit_cloud_* \
        target/release/deps/jj* \
        target/release/deps/xtask* \
        target/release/jj* \
        target/release/xtask*

COPY common ./common
COPY lib ./lib
COPY server ./server
COPY daemon ./daemon
COPY cli ./cli
COPY testutils ./testutils
COPY xtask ./xtask

# common/build.rs falls back to vendored protoc, so external protobuf-compiler is optional.
RUN rm -rf target/release/build/cc-common* target/release/build/cc-lib* target/release/build/jj-commit-cloud* target/release/build/cli* target/release/build/xtask* target/release/.fingerprint/cc-common* target/release/.fingerprint/cc-lib* target/release/.fingerprint/jj-commit-cloud* target/release/.fingerprint/cli* target/release/.fingerprint/xtask* \
    && find common lib server daemon cli testutils xtask -type f -exec touch {} + \
    && mkdir -p /data \
    && chmod 0700 /data \
    && cargo build --locked --release \
    -p jj-commit-cloud-server --bin jj-cc-server \
    -p jj-commit-cloud-daemon --bin jj-cc-daemon \
    -p cli --bin jj \
    -p xtask --bin xtask

FROM docker.io/library/debian:bookworm-slim AS cli
RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates \
    && rm -rf /var/lib/apt/lists/* \
    && mkdir -p /home/jj \
    && chown 65534:65534 /home/jj \
    && chmod 0700 /home/jj
COPY --from=builder /app/target/release/jj /usr/local/bin/jj
COPY --from=builder /app/target/release/jj-cc-daemon /usr/local/bin/jj-cc-daemon
COPY --from=builder /app/target/release/xtask /usr/local/bin/xtask
ENV HOME=/home/jj
USER 65534:65534
ENTRYPOINT ["jj"]

FROM scratch AS bins
COPY --from=builder /app/target/release/jj-cc-server /jj-cc-server
COPY --from=builder /app/target/release/jj-cc-daemon /jj-cc-daemon
COPY --from=builder /app/target/release/jj /jj

FROM gcr.io/distroless/cc-debian12:nonroot AS server
COPY --from=builder --chown=65532:65532 --chmod=0700 /data /data
COPY --from=builder /app/target/release/jj-cc-server /usr/local/bin/jj-cc-server

USER 65532:65532
VOLUME ["/data"]
ENV HOST=0.0.0.0
ENV PORT=8080
# Leave BLOB_PATH unset so STORE_TYPE=sqlite defaults to /data/commit_cloud.db.blobs,
# STORE_TYPE=memory defaults to in-memory blobs, and STORE_TYPE=spanner without
# GCS_BUCKET or BLOB_PATH fails closed at startup.
ENV STORE_TYPE=sqlite SQLITE_PATH=/data/commit_cloud.db
EXPOSE 8080

# Non-loopback HOST=0.0.0.0 fails closed unless authentication is configured via
# static Bearer tokens (JJ_CC_SERVER_AUTH_TOKEN or JJ_CC_SERVER_AUTH_TOKEN_FILE),
# OIDC JWT validation (JJ_CC_AUTH_OIDC_ISSUER + JJ_CC_AUTH_OIDC_AUDIENCE), or
# Kubernetes TokenReview (JJ_CC_AUTH_K8S_TOKENREVIEW_URL), or unless
# JJ_CC_ALLOW_UNAUTHENTICATED=true is set behind an authenticating proxy (for example
# Cloud Run IAM, Identity-Aware Proxy, or an isolated container network).
# Server flags from `jj-cc-server --help` read directly from environment variables,
# including HOST, PORT, METRICS_LISTEN, STORE_TYPE, BLOB_STORE, SQLITE_PATH,
# POSTGRES_URL / DATABASE_URL, SPANNER_DATABASE, BLOB_PATH, S3_BUCKET / S3_ENDPOINT,
# GCS_BUCKET / GCS_PEER_BUCKETS, and the auth variables above.
ENTRYPOINT ["/usr/local/bin/jj-cc-server"]
