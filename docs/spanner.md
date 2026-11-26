# Running jj-cc-server on Cloud Spanner

## Summary

jj-cc-server stores repository metadata (projects, repositories, commits, trees, symlinks, operations, views, operation heads, workspaces, and blob records) in [Cloud Spanner](https://cloud.google.com/spanner) when started with `--store-type spanner`, paired with an explicit blob store (`--blob-path` for local disk storage or `--blob-store=memory` for throwaway tests). Workspaces that pass the same `--repo-id` to `jj cc init` share one operation log: a commit made in one workspace shows up in `jj log` of the others. Concurrent writers use lock-free additive `UpdateOpHeads` inside serializable Spanner transactions: two writers starting from the same operation head both succeed with zero lost updates, keeping both sibling heads until `ReconcileOpHeads` merges them back to a single head.

Follow these steps to build the server and the jj CLI, create a database on Cloud Spanner or the Spanner emulator, start the server with an explicit blob store, connect two workspaces, verify shared history and concurrent-writer reconciliation, and clean up. Every step lists the output to expect.

## Prerequisites

| Tool | Version | Notes |
| --- | --- | --- |
| Linux, x86_64 | any | the steps were run on Linux |
| git | any | |
| Rust toolchain via [rustup](https://rustup.rs) | 1.97.1 | `rustup override set 1.97.1` in the checkout; with Rust 1.100 and newer, some `cargo test` helper lookups fail |
| protoc | none needed | the build uses a vendored protoc unless `protoc` is on `PATH` or `PROTOC` is set |
| [gcloud CLI](https://cloud.google.com/sdk/docs/install) | tested with 593.0.0 | Cloud Spanner only |
| Docker or Podman | any | Spanner emulator only; image `gcr.io/cloud-spanner-emulator/emulator:1.5.59` |

## Build

Build from the checkout that contains this file:

```sh
git clone https://github.com/jj-vcs/jj-commit-cloud
cd jj-commit-cloud
rustup override set 1.97.1
cargo build --release --bin jj-cc-server --bin jj --bin jj-cc-daemon
export CC_SRC="$PWD"
export PATH="$CC_SRC/target/release:$PATH"
```

Expected output, last line (the first build takes several minutes):

```
    Finished `release` profile [optimized] target(s) in …
```

The jj built here embeds the `jj cc` commands and starts jj-cc-daemon from the same directory on demand. Putting `target/release` first on `PATH` makes every jj command below use this build, not a system jj.

## Create the database

Pick one of the two options. Each ends with a database path for `--spanner-database` (alias `--spanner-db`), exported as `SPANNER_DB`.

### Cloud Spanner

Needs a GCP project and rights to create Spanner instances. The instance is billed by the hour until it is deleted (about $0.10 per hour at 100 processing units, the smallest size); instances cannot be paused.

```sh
export PROJECT=my-project                # your GCP project
export INSTANCE=jj-cc                    # any instance name
gcloud auth login
gcloud services enable spanner.googleapis.com --project "$PROJECT"
gcloud spanner instances create "$INSTANCE" --project "$PROJECT" \
    --config=regional-europe-west3 --processing-units=100 --description="jj-cc-server"
gcloud spanner databases create jj-cc --instance="$INSTANCE" --project "$PROJECT"
grep -v '^\s*--' "$CC_SRC/server/sql/spanner_schema.sql" > /tmp/jj-cc-schema.sql
gcloud spanner databases ddl update jj-cc --instance="$INSTANCE" --project "$PROJECT" \
    --ddl-file=/tmp/jj-cc-schema.sql
gcloud auth application-default login    # credentials for a server on this machine
export SPANNER_DB="projects/$PROJECT/instances/$INSTANCE/databases/jj-cc"
```

Expected output: `gcloud spanner databases ddl update` returns after the schema is in place. Check it:

```sh
gcloud spanner databases ddl describe jj-cc --instance="$INSTANCE" --project "$PROJECT" | grep -c 'CREATE TABLE'
```

```
11
```

The schema in [server/sql/spanner_schema.sql](../server/sql/spanner_schema.sql) has `--` comment lines, which `grep` removes before gcloud reads the statements. A server on a Compute Engine VM uses the VM's service account in place of `gcloud auth application-default login`; grant it `roles/spanner.databaseUser` on the database.

### Spanner emulator

Runs locally with no GCP account. Its data lives in memory and is gone when the container stops.

```sh
docker run -d --name spanner-emulator -p 9010:9010 -p 9020:9020 \
    gcr.io/cloud-spanner-emulator/emulator:1.5.59
curl -s -X POST localhost:9020/v1/projects/test-project/instances \
    -d '{"instanceId":"test-instance","instance":{"config":"emulator-config","displayName":"test","nodeCount":1}}'
curl -s -X POST localhost:9020/v1/projects/test-project/instances/test-instance/databases \
    -d '{"createStatement":"CREATE DATABASE `jj-cc`"}'
export SPANNER_EMULATOR_HOST=localhost:9010
export SPANNER_DB="projects/test-project/instances/test-instance/databases/jj-cc"
```

Expected output: each `curl` prints a JSON operation whose `name` ends in `/operations/_auto0` or similar. With Podman, replace `docker` with `podman`. Passing `--spanner-auto-migrate` when starting jj-cc-server applies [server/sql/spanner_schema.sql](../server/sql/spanner_schema.sql) on startup (and creates the database if it is missing).

## Start the server

In a terminal that has `CC_SRC`, `PATH`, `SPANNER_DB` and, for the emulator, `SPANNER_EMULATOR_HOST` set, start jj-cc-server with an explicit blob store (`--blob-path` for local disk storage or `--blob-store=memory` for throwaway tests).

Against Cloud Spanner (using the schema already applied with `gcloud` above):

```sh
jj-cc-server --port 8080 --store-type spanner \
    --spanner-database "$SPANNER_DB" \
    --blob-path /tmp/jj-cc-blobs
```

Against the Spanner emulator, add `--spanner-auto-migrate` so the server creates the tables on startup. Replace `--blob-path /tmp/jj-cc-blobs` with `--blob-store=memory` for throwaway runs where blobs do not need to survive restarts:

```sh
jj-cc-server --port 8080 --store-type spanner \
    --spanner-database "$SPANNER_DB" \
    --spanner-auto-migrate \
    --blob-path /tmp/jj-cc-blobs
```

Expected output, after a few seconds:

```
INFO jj_cc_server: Connecting to Cloud Spanner database 'projects/…/databases/jj-cc'
INFO jj_cc_server: Opening local filesystem blob store at /tmp/jj-cc-blobs
INFO jj_cc_server: jj-cc-server listening on 127.0.0.1:8080
```

Without `--spanner-auto-migrate`, the server performs no DDL on startup, so `roles/spanner.databaseUser` is sufficient on Cloud Spanner. Stop the server with Ctrl+C.

## Connect two workspaces

In a second terminal, with `CC_SRC` and `PATH` set as in the build step. `--project-id` names the pool of shared objects, `--repo-id` the repository (one operation log), and `--workspace-name` must differ per workspace.

```sh
export JJ_USER="Your Name" JJ_EMAIL=you@example.com
export SERVER=http://127.0.0.1:8080 REPO="repo-$(date +%s)"

mkdir -p /tmp/jj-cc/a && cd /tmp/jj-cc/a
jj cc init --server "$SERVER" --project-id test-project --repo-id "$REPO" --create \
    --workspace-name a --working-copy-type local .
echo "hello" > hello.txt
jj describe -m "first commit, from workspace a"

mkdir -p /tmp/jj-cc/b && cd /tmp/jj-cc/b
jj cc init --server "$SERVER" --project-id test-project --repo-id "$REPO" \
    --workspace-name b --working-copy-type local .
```

Expected output: each `jj cc init` prints `Initialized remote Commit Cloud repository`, and `jj describe` prints:

```
Working copy  (@) now at: … first commit, from workspace a
Parent commit (@-)      : zzzzzzzz 00000000 (empty) (no description set)
```

`--create` is only for the first workspace of a repository. `--working-copy-type local` keeps the files in the workspace directory; the default, `commit_cloud`, keeps the working-copy state on the server.

## Verify shared history

In workspace b:

```sh
cd /tmp/jj-cc/b
jj log -r 'all()'
jj op log
```

Expected output, with other ids:

```
@  rxuspklz you@example.com 2026-11-25 16:27:20 b@ a28758c1
│  (empty) (no description set)
│ ○  zrllpyko you@example.com 2026-11-25 16:27:20 a@ 45bf662c
├─╯  first commit, from workspace a
◆  zzzzzzzz root() 00000000
```

```
@    6acc89818af1 @ now, lasted 9 milliseconds
├─╮  reconcile divergent operations
○ │  7e1b4b7eed7e … a@ now, lasted 9 milliseconds
│ │  describe commit 27dd5ac4c978dd01605048b0170c0dc850ff106e
○ │  41c5813ed316 … a@ now, lasted 11 milliseconds
│ │  snapshot working copy
○ │  dc124b733c9c … now, lasted 10 milliseconds
│ │  add workspace 'a'
│ ○  1317389bca9f … now, lasted 13 milliseconds
├─╯  add workspace 'b'
○  000000000000 root()
```

Workspace b's first operation starts from the root operation while workspace a has already moved on, so the operation log forks; the server merges the fork the next time a client loads the repository (`reconcile divergent operations`). The same check runs as a test against the memory and SQLite stores: `cargo test --test test_two_clones` ([cli/tests/test_two_clones.rs](../cli/tests/test_two_clones.rs)).

## Check concurrent writers

From the checkout, while the server runs:

```sh
cd "$CC_SRC"
CC_RACE_SERVER_URL=http://127.0.0.1:8080 cargo test --release --test test_two_writer_race -- --ignored
```

Expected output, at the end:

```
test test_two_writer_race_external_server ... ok

test result: ok. 1 passed; 0 failed; 0 ignored; 0 measured; 1 filtered out; finished in …
```

The test ([cli/tests/test_two_writer_race.rs](../cli/tests/test_two_writer_race.rs)) runs 50 rounds in which two clients start from the same base operation head, synchronize at a barrier, and send `UpdateOpHeads` at the same time. Every round verifies that both lock-free additive `UpdateOpHeads` calls succeed with zero lost updates, that `GetOpHeads` retains both concurrent sibling heads, and that `ReconcileOpHeads` merges the divergent heads back to a single operation head.

## Stop and clean up

```sh
# Ctrl+C in the server terminal, then:
pkill -f "jj-cc-daemon --server http://127.0.0.1:8080 " || true
rm -rf /tmp/jj-cc /tmp/jj-cc-blobs /tmp/jj-cc-schema.sql
# Cloud Spanner: delete the instance; this stops the billing and deletes the data
gcloud spanner instances delete "$INSTANCE" --project "$PROJECT"
# Emulator: remove the container
docker rm -f spanner-emulator && unset SPANNER_EMULATOR_HOST
```

## Common problems

* **The server exits with `--store-type=spanner requires an explicit blob store`:** Spanner stores metadata and requires an explicit blob store so file contents do not land in memory by mistake. Pass `--blob-path /tmp/jj-cc-blobs` (or `BLOB_PATH`) for local disk storage, or `--blob-store=memory` for throwaway tests.
* **The server logs `cannot create the authentication headers` and never prints `listening`:** the server has no Google credentials. Run `gcloud auth application-default login` on the machine that runs the server, or run it on a VM whose service account has `roles/spanner.databaseUser` on the database. Check that `SPANNER_EMULATOR_HOST` is unset when the target is Cloud Spanner, and set when it is the emulator.
* **`jj cc init` fails with `Failed to connect to Commit Cloud server: transport error`:** nothing listens on `--server` yet. Wait for `jj-cc-server listening on 127.0.0.1:8080`, and check that the port in `--server` matches `--port`; when `--spanner-auto-migrate` is set, the first start can take several seconds while the server applies the schema.
