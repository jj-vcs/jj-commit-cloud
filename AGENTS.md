# Agent Instructions

Use these rules when reading, editing, testing, or reviewing code in [jj-commit-cloud](https://github.com/jj-vcs/jj-commit-cloud). When drafting or editing design documents, RFCs, or architecture guides under [docs/](docs/) or [README.md](README.md), also follow [docs/AGENTS.md](docs/AGENTS.md).

## Workspace Crates and Service Surfaces

* **Protocol Definitions ([common/](common/), `cc-common`):** Compiles `BackendService` ([common/proto/backend.proto](common/proto/backend.proto)), `OpStoreService` ([common/proto/op_store.proto](common/proto/op_store.proto)), and `WorkspaceService` ([common/proto/workspace.proto](common/proto/workspace.proto)) into `tonic` stubs and exports digest-length and virtual-root constants.
* **Client Storage Backends ([lib/](lib/), `cc-lib`):** Implements `jj_lib` storage traits (`CommitCloudBackend`, `CommitCloudOpStore`, `CommitCloudOpHeadsStore`, `CommitCloudIndexStore`, `CommitCloudWorkingCopy`) over gRPC with optional Unix domain socket routing through jj-cc-daemon.
* **Commit Cloud Server ([server/](server/), `jj-commit-cloud-server`, binary jj-cc-server):** Serves gRPC endpoints, computes canonical object digests via [gitoxide](https://github.com/GitoxideLabs/gitoxide), reconciles divergent operation heads, and persists repository state behind `CompositeStore` coordinating `MetadataStore` (`MemoryMetadataStore`, `SqliteMetadataStore`, `SpannerMetadataStore`) and `BlobStore` (`MemoryBlobStore`, `LocalFsBlobStore`, `GcsBlobStore`, `ReplicatedBlobStore`).
* **Custom CLI ([cli/](cli/), binary jj):** Embeds `jj_cli` alongside the `jj cc` subcommand family while keeping local `GitBackend` repositories working unchanged.
* **Local Proxy Daemon ([daemon/](daemon/), `jj-commit-cloud-daemon`, binary jj-cc-daemon):** Runs a local Unix domain socket gRPC proxy forwarding `BackendService`, `OpStoreService`, and `WorkspaceService` calls over a shared upstream connection.
* **Integration Test Utilities ([testutils/](testutils/)):** Builds and spawns ephemeral jj-cc-server and jj-cc-daemon instances for automated tests.

## Codebase, Proto, and Comment Conventions

* **Match Implementation Scope:** Keep code changes focused on the requested behavior and active storage backend. Do not add multi-region coordination or high-scale distributed machinery when editing in-memory or single-service code unless specified by the task or a design doc in [docs/](docs/).
* **License and Module-Level `//!` Invariant Headers in Rust:** Every new source file carries the Apache 2.0 header from [docs/contributing.md](docs/contributing.md#source-code-headers). New modules, and modules whose invariants you change, carry a `//!` docblock of at most 25 lines after the Apache header, covering whichever topics apply:
  * **Authority and Crash-Safety Ordering:** Write-before-publish ordering across objects, views, operations, and active operation heads, and how interrupted writes recover.
  * **Concurrency and Merge Invariants:** Locking rules, compare-and-swap semantics on operation heads, and how divergent heads reconcile.
  * **Cross-Process and RPC Boundaries:** gRPC request/response framing, streaming invariants, and retry safety.
* **Proto-First Service Contracts:** In `.proto` files under [common/proto/](common/proto/), document idempotency requirements, transaction isolation expectations, pagination cursor invariants, and gRPC status codes on `service`, `rpc`, and `message` definitions rather than burying them in Rust handlers.
* **Write "Why", Not "What" Comments:** Omit line-by-line comments that restate Rust syntax.
* **Keep Agent Docs Compact:** Keep `AGENTS.md` files under 1,500 tokens with no manual line-number maps or perishable file lists, and extend crate, proto, or binary lists in the commits that add them.
* **Actionable CLI Errors:** Teach [jj](https://github.com/jj-vcs/jj)'s mental model in CLI error messages (working-copy-as-a-commit `@`, bookmarks, algebraic conflicts `Merge<TreeId>`, and the operation log via `jj op log` and `jj undo`) with runnable remediation commands.

## Build, Test, and Version Control

* **Build and Test Prerequisites:** Compiling `cc-common` ([common/build.rs](common/build.rs)) honors `PROTOC` or `protoc` from `PATH` when available and falls back to `protoc-bin-vendored` otherwise. Running `cargo test` builds jj-cc-server and jj-cc-daemon through a nested `cargo build` in [testutils/build.rs](testutils/build.rs).
* **No Co-Author or Attribution Trailers:** Commit messages carry no `Co-authored-by:` trailer and no other trailer that credits a tool, a coding agent, or a person. Every author of a commit must have signed the Google CLA, and the CLA check blocks the merge of a pull request whose commits name an author who has not, including a coding agent. Remove any such trailer that a tool adds by default; fix an existing commit with `jj describe` before pushing.
* **Auto-Format Modified Files With `jj fix`:** Run `jj fix -s @` (or plain `jj fix` across a commit stack) after editing Rust source files so formatting stays scoped to modified files without introducing rebase conflicts, and reserve `cargo fmt --all` for standalone workspace-wide formatting commits. Configure `fix.tools.rustfmt` once per checkout with `jj config set --repo fix.tools.rustfmt '{ command = ["rustfmt", "--edition", "2024", "--emit", "stdout"], patterns = ["glob:**/*.rs"] }'`.
