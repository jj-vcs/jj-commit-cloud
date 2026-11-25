# Running jj-cc-server on Cloud Spanner

## Summary

jj-cc-server keeps commits, trees, files, operations, views and operation heads in [Cloud Spanner](https://cloud.google.com/spanner) when started with `--store-type spanner`. Workspaces that pass the same `--repo-id` to `jj cc init` share one operation log: a commit made in one workspace shows up in `jj log` of the others. Two writers that expect the same operation head cannot both win; the second gets a CAS conflict (gRPC `ABORTED`).

The sections below build the server and the jj CLI, create a database (Cloud Spanner, or the Spanner emulator with no GCP account), start the server, connect two workspaces, check that they share history and that concurrent writers are serialized, and remove everything again. Every step lists the output to expect.

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

Pick one of the two options. Each ends with a database path for `--spanner-db`, exported as `SPANNER_DB`.

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
grep -v '^\s*--' "$CC_SRC/server/db/schema_spanner.sql" > /tmp/jj-cc-schema.sql
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
10
```

The schema in [server/db/schema_spanner.sql](../server/db/schema_spanner.sql) has `--` comment lines, which the `grep` removes before gcloud reads the statements. A server on a Compute Engine VM uses the VM's service account in place of `gcloud auth application-default login`; grant it `roles/spanner.databaseUser` on the database.

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

Expected output: each `curl` prints a JSON operation whose `name` ends in `/operations/_auto0` or similar. With Podman, replace `docker` with `podman`. The server creates the tables itself on the emulator at start.

## Start the server

In a terminal that has `CC_SRC`, `PATH`, `SPANNER_DB` and, for the emulator, `SPANNER_EMULATOR_HOST` set:

```sh
jj-cc-server --port 8080 --store-type spanner --spanner-db "$SPANNER_DB"
```

Expected output, after a few seconds:

```
INFO jj_cc_server: Connecting to Cloud Spanner at 'projects/…/databases/jj-cc'
INFO jj_cc_server::store::spannerstore: Ensuring Spanner database schema tables exist for '…'...
INFO jj_cc_server: jj-cc-server listening on 127.0.0.1:8080
```

On Cloud Spanner with only `roles/spanner.databaseUser`, the server also logs `Spanner auto-schema initialization notice`, because it may not change the schema; the schema applied with gcloud above is the one in use. Stop the server with Ctrl+C.

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

The test ([cli/tests/test_two_writer_race.rs](../cli/tests/test_two_writer_race.rs)) runs 50 rounds in which two clients send `UpdateOpHeads` at the same moment, both expecting the same head, and fails unless every round gives one success and one `ABORTED`.

## Stop and clean up

```sh
# Ctrl+C in the server terminal, then:
pkill -f "jj-cc-daemon --server http://127.0.0.1:8080 " || true
rm -rf /tmp/jj-cc /tmp/jj-cc-schema.sql
# Cloud Spanner: delete the instance; this stops the billing and deletes the data
gcloud spanner instances delete "$INSTANCE" --project "$PROJECT"
# Emulator: remove the container
docker rm -f spanner-emulator && unset SPANNER_EMULATOR_HOST
```

## Common problems

* **The server logs `cannot create the authentication headers` and never prints `listening`:** the server has no Google credentials. Run `gcloud auth application-default login` on the machine that runs the server, or run it on a VM whose service account has `roles/spanner.databaseUser` on the database. Check that `SPANNER_EMULATOR_HOST` is unset when the target is Cloud Spanner, and set when it is the emulator.
* **`jj cc init` fails with `Failed to connect to Commit Cloud server: transport error`:** nothing listens on `--server` yet. Wait for `jj-cc-server listening on 127.0.0.1:8080`, and check that the port in `--server` matches `--port`; on Cloud Spanner the first start can take several seconds while the server checks the schema.
* **`jj cc init` for the second workspace fails with `CAS conflict: write race detected on stale old_op_head_ids: head 00000000000000000000000000000000`:** the server was built from a revision older than the change that lets a new workspace join an existing repository. Rebuild from the checkout that contains this file.
