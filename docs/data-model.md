# Commit Cloud Data Model

> [!NOTE]
> Milestone 1 target data-model specification. On `main` today, [common/proto/](../common/proto/) and [server/src/store/](../server/src/store/) implement single-repository in-memory tables; project-scoped object pools (`Projects` and `ProjectBlobs`), `MetadataStore` and `BlobStore` schemas for SQLite, Cloud Spanner, and Cloud Storage, and `Workspace` records specify the target Milestone 1 layout landing across incremental pull requests.

## Summary

jj-commit-cloud represents [jj](https://github.com/jj-vcs/jj)'s core version control entities - commits, directory trees, files, symlinks, views, and operation logs - in remote cloud storage while preserving Git-compatible object hashes in Milestone 1 and preparing length-agile keys plus `Commit.external_ids` for future prolly-tree and tree-delta formats.

Data is organized across three scopes so a single Commit Cloud deployment can host many unrelated codebases alongside shared multi-agent workspaces:

* **Projects (`project_id`):** Own the immutable graph of commits, directory trees, symlinks, and file-ownership links. Unrelated codebases live in separate projects with disjoint commit graphs.
* **Repositories (`repo_id`):** Belong to a parent project and own the mutable per-repository state - active operation heads, the append-only operation log, view snapshots, and attached workspaces.
* **Global Content-Addressed Blobs (`BlobId`):** Deduplicate raw file byte streams by content hash in `BlobStore`, gated per project (`ProjectBlobs`) so projects never share file visibility unless both projects have written the file.

For generic jj concepts such as `ChangeId` versus `CommitId`, algebraic conflict arithmetic, bookmarks, and `jj undo`, see upstream's [Glossary](https://docs.jj-vcs.dev/latest/glossary/), [Operation Log](https://docs.jj-vcs.dev/latest/operation-log/), and [Technical Conflicts](https://docs.jj-vcs.dev/latest/technical/conflicts/); for overall system architecture, see [System Design](design.md).

## Multi-Repository Partitioning and Disjoint Histories

A local `.git/objects` directory stores objects for one repository on one machine, whereas Commit Cloud stores data for many repositories at once. Hosted repositories may be unrelated codebases with no shared history, or they may be dozens of developer and agent sandboxes collaborating on the same codebase.

<!--
Diagram styling and update instructions:
- Uses compact Mermaid flowchart LR (12px font, tight spacing) with per-repository entities (#fff3e0 / #e65100), project-scoped commit-graph entities (#e1f5fe / #01579b), and content-addressed blob storage (#e8f5e9 / #2e7d32).
- To update: edit the node or edge declarations below and preview the rendered Mermaid block in Markdown.
-->
```mermaid
%%{init: {"themeVariables": {"fontSize": "12px"}, "flowchart": {"nodeSpacing": 14, "rankSpacing": 24, "padding": 6, "diagramPadding": 8}}}%%
flowchart LR
    classDef oplog fill:#fff3e0,stroke:#e65100,color:#000;
    classDef cas fill:#e1f5fe,stroke:#01579b,color:#000;
    classDef storage fill:#e8f5e9,stroke:#2e7d32,color:#000;

    Repo["Repository<br/>repo_id, OpHeads"]:::oplog
    Op["Operation<br/>OpLog DAG"]:::oplog
    View["View<br/>heads, bookmarks, @"]:::oplog
    Ws["Workspace<br/>@, tree_ids"]:::oplog

    Proj["Project<br/>project_id"]:::cas
    Commit["Commit<br/>parents, change_id"]:::cas
    Tree["Tree<br/>sorted entries"]:::cas
    Leaf["File & Symlink<br/>ProjectBlobs"]:::cas
    Blob["BlobStore<br/>blobs/aa/bb/hex"]:::storage

    Repo --> Op --> View
    Repo --> Ws
    Repo --> Proj
    View -.-> Commit
    Ws -.-> Commit
    Proj --> Commit --> Tree --> Leaf --> Blob
```

Suppose one Commit Cloud cluster hosts two unrelated codebases - `project_id = "kernel"` (`repo-kernel`) and `project_id = "webapp"` (`repo-alice` and `repo-agent-7`) - and both codebases happen to contain an identical `LICENSE` file with Git blob hash $B_L$:

* **Disjoint Commit Graphs Across Projects:** `"kernel"` commits and trees are keyed under `"kernel"`, while `"webapp"` commits and trees are keyed under `"webapp"`. Listing or resolving commits in `repo-alice` scans only `"webapp"`, never `"kernel"`.
* **Zero-Copy Sharing Within a Project:** When `repo-agent-7` writes a new commit $C_{w3}$ in `"webapp"`, `repo-alice` can inspect or cherry-pick $C_{w3}$ without re-uploading any trees or files, while `repo-alice`'s bookmarks and operation log stay untouched until Alice chooses to reference $C_{w3}$.
* **Deduplicated Storage With Isolated Ownership:** `BlobStore` stores the bytes for $B_L$ once at `blobs/<aa>/<bb>/<B_L>`, while `ProjectBlobs` records separate `(kernel, B_L)` and `(webapp, B_L)` ownership rows. `ProjectBlobs` rows are recorded only after a project uploads the file bytes (never from a hash-only existence check), so a third project `"firmware"` that has never uploaded $B_L$ receives `NOT_FOUND` if it attempts to read $B_L$.

## Core Entities and Relationships

### Operation

An operation represents a single state transition in a repository - such as snapshotting edited files, describing a change, rebasing a stack, or moving a bookmark. Operations form a per-repository directed acyclic graph in the operation log, recording start and end timestamps, user and host metadata, commit predecessor lineage (`commit_predecessors`), and a pointer to the resulting `View`. Each repository tracks its active tip operation IDs in `OpHeads`; under concurrent edits `OpHeads` holds multiple sibling IDs until reconciled into a merged operation.

### View

A view captures what a repository sees at a specific operation: its visible head commits (`head_ids`), local and remote bookmarks (`local_bookmarks` and `remote_bookmarks`, stored as added and removed commit lists so conflicting bookmark moves survive merges), and the current working-copy commit (`@`) for each workspace (`wc_commit_ids`). When a shared project pool accumulates hundreds of thousands of commits across developers and sandboxes, a repository's `View` acts as a compact filter defining the exact subgraph visible to `jj log` in that repository.

### Workspace

A workspace record in `workspace.proto` under [common/proto/](../common/proto/) tracks an attached local or virtual checkout keyed by `(repo_id, user, workspace_name)`. It records the checkout's materialized `commit_id`, `operation_id`, root `tree_ids` - preserving every side of a `Merge<TreeId>` when a conflicted commit is checked out alongside primary `tree_id` - and active `sparse_patterns`, letting tools and agent orchestrators inspect workspace health across machines.

### Commit

A commit is an immutable, insert-only snapshot in the commit graph. It records parent commit IDs (`parent_commit_ids`), rewrite predecessors (`predecessors`), logical `change_id`, commit description, author and committer signatures, optional cryptographic signature (`secure_sig`), and root directory tree (`root_tree_id`). A resolved commit stores one `TreeId`, whereas a commit containing an unresolved merge conflict stores an odd number $2N + 1$ of tree IDs alternating $N + 1$ positive and $N$ negative terms (`Merge<TreeId>`) alongside `conflict_labels`. Imported Git commits also retain their verified raw header bytes (`raw_git_commit`) and external VCS digest map (`external_ids`, such as `"git"`) for bit-for-bit Git export. Persistent metadata stores also record each commit's topological generation number ($\max(\text{parent generation}) + 1$, with 1 for root-parented commits) to bound ancestry walks.

### Tree

A tree represents a single directory, mapping single-component entry names to child `TreeValue` targets: a regular or executable `File`, a `Symlink`, a child subdirectory `Tree`, or a Git submodule commit gitlink. `TreeId` is path-independent, matching upstream jj: two directories at different paths with identical entry names, modes, and child hashes share the same `TreeId`, and unmodified subdirectories are reused across commits without copying entries. Stored and cached `Tree` payloads keep entries sorted in `RepoPathComponent` UTF-8 byte order (`"foo" < "foo.rs"`) so `jj_lib`'s binary-search lookups work directly on the wire representation.

### File and Symlink

File and symlink entries form the leaves of a directory tree. Standard open-source `jj_lib` tree entries record only a file's content hash and executable bit, omitting file byte length - which would force virtual filesystems such as [jj-vfs](https://github.com/jj-vcs/jj-vfs) to open or stream every file in a directory just to answer `stat()` (`ls -l`). Commit Cloud tracks each blob's byte size (`ContentSize`) in `MetadataStore` and populates `File.size_bytes` on `ReadTree` responses without altering canonical Git tree hashes. Short UTF-8 symlink target paths live directly in `MetadataStore`, while file byte payloads live in `BlobStore`.

## Object Identity and Planned Hash Evolution

Canonical identifiers are computed and verified on the server in [server/src/hash_utils.rs](../server/src/hash_utils.rs) using the constants in [common/src/lib.rs](../common/src/lib.rs):

| Identifier | Milestone 1 Canonical Identity | Virtual Root Constant |
| :--- | :--- | :--- |
| `FileId` | Git-compatible `blob` digest (`"blob <len>\0" + content`) | None |
| `SymlinkId` | Git-compatible `blob` digest over UTF-8 target path | None |
| `TreeId` | Git-compatible `tree` digest over `<mode> <name>\0<oid>` entries sorted with trailing `/` on directories: `100644`/`100755` file, `120000` symlink, `40000` tree (`git ls-tree` displays `040000`), `160000` submodule | `EMPTY_TREE_ID` (canonical empty tree digest) |
| `CommitId` | Git-compatible `commit` digest omitting the synthetic all-zero root parent, with `change-id` (`z-k` reverse-hex), `jj:trees`, `jj:conflict-labels`, and `gpgsig` headers | `ROOT_COMMIT_ID` (all-zero sentinel) |
| `ChangeId` | Stable 128-bit logical change ID (rendered in `z-k` reverse-hex) | Root change ID (`zzzzzzzz...`) |
| `ViewId` | Deterministic content digest over key-sorted, 8-byte length-prefixed `View` fields | `ROOT_VIEW_ID` (all-zero sentinel) |
| `OperationId` | Deterministic content digest over `view_id`, parents, sorted metadata attributes, and predecessors | `ROOT_OPERATION_ID` (all-zero sentinel) |

Reads of the four virtual root constants (`ROOT_COMMIT_ID`, `EMPTY_TREE_ID`, `ROOT_VIEW_ID`, and `ROOT_OPERATION_ID`) are synthesized in memory without database lookups (on `main` today, the server synthesizes `EMPTY_TREE_ID`, `ROOT_VIEW_ID`, and `ROOT_OPERATION_ID`, while `CommitCloudBackend` synthesizes `ROOT_COMMIT_ID` client-side; Milestone 1 synthesizes `ROOT_COMMIT_ID` on the server as well), with `ROOT_VIEW` exposing `head_ids = [ROOT_COMMIT_ID]` as every new repository's initial graph anchor.

### Decoupling Internal Hashes From External VCS Hashes

Addressing commits and trees by Git-compatible object digests makes Milestone 1 Git import and export translation-free, but Git's flat directory tree hash couples every `TreeId` and `CommitId` to an $O(N)$ serialization of all entries in a directory. Supporting prolly-tree directory nodes and compact tree-delta commits on wide monorepo directories requires decoupling Commit Cloud's canonical internal digests from Git's Merkle format:

* **Length-Agile Keys (`BYTES(64)`):** Database columns and Protocol Buffer fields store object IDs as variable-length byte slices up to 64 bytes rather than fixed-width arrays tied to one hash algorithm.
* **Imported and Post-Hoc External IDs:** Following [Mononoke](https://github.com/facebookexperimental/mononoke)'s Bonsai changeset model and derived hash-mapping tables, Milestone 1 adds an external-ID map (`external_ids` on `Commit` in [backend.proto](../common/proto/backend.proto)) keyed by external VCS scheme (such as `"git"` or `"perforce-cl"`). Git imports record their Git commit ID at ingest; once native prolly-tree and tree-delta commits are enabled, commits authored in Commit Cloud will receive native internal IDs at write time and attach derived Git or Perforce hashes after the fact during export or background mirror sync.

## Comparison With Local Git Storage

Protocol Buffer contracts in [backend.proto](../common/proto/backend.proto), [op_store.proto](../common/proto/op_store.proto), and `workspace.proto` under [common/proto/](../common/proto/) carry jj's domain model across gRPC without flattening conflicts or rewrite history into lossy local files:

| Dimension | Local `GitBackend` (`.git/` + `.jj/`) | jj-commit-cloud |
| :--- | :--- | :--- |
| **Repository Hosting** | One repository per `.git/` directory on disk | Many disjoint projects and shared-pool repositories on one server |
| **Conflicts** | Stored in Git trees + `.jj/repo/store/extra/` sidecar tables | Stored natively as `Merge<TreeId>` (`root_tree_id`) and `RefTarget` add/remove lists |
| **File Metadata & Sizes** | Content-addressed packfiles; reading byte size requires opening object headers | Content-addressed blobs gated per project; `File.size_bytes` returned inline on `ReadTree` |
| **Ancestry & Index Sync** | Computed locally in `.jj/repo/index/` | Stored `Generation` numbers plus incremental local indexing from view heads |
| **Operation Log & Heads** | Local files under `.jj/repo/op_store/` and `op_heads/` | Transactional database rows with lock-free sibling head preservation and server-side merge |

## Physical Storage Layout

### Cloud Spanner Table Layout

The Cloud Spanner DDL (`server/sql/spanner_schema.sql`) stores unbounded object graphs and operation histories in independent top-level tables with scope-first composite keys (`(ProjectId, <ObjectId>)` and `(RepoId, <Id>)`) so rows group by project or repository without piling millions of commits, trees, or operations under one parent row. Interleaving is reserved for bounded parent-child pairs read together - `TreeEntries` under `Trees` and `Workspaces` under `Repositories`:

<!--
Diagram styling and update instructions:
- Uses compact Mermaid flowchart LR (12px font, tight spacing) showing top-level per-repository tables (#fff3e0 / #e65100), top-level per-project object tables (#e1f5fe / #01579b), and top-level global blob metadata (#e8f5e9 / #2e7d32), with INTERLEAVE arrows only for TreeEntries in Trees and Workspaces in Repositories.
- To update: edit the table nodes or edges below when server/sql/spanner_schema.sql changes and preview the rendered Mermaid block in Markdown.
-->
```mermaid
%%{init: {"themeVariables": {"fontSize": "12px"}, "flowchart": {"nodeSpacing": 14, "rankSpacing": 26, "padding": 6, "diagramPadding": 8}}}%%
flowchart LR
    classDef oplog fill:#fff3e0,stroke:#e65100,color:#000;
    classDef cas fill:#e1f5fe,stroke:#01579b,color:#000;
    classDef storage fill:#e8f5e9,stroke:#2e7d32,color:#000;

    subgraph RepoTables["Per-Repository Tables (Top-Level)"]
        Repos["Repositories<br/>PK: (RepoId)"]:::oplog
        Workspaces["Workspaces<br/>PK: (RepoId, User, Name)"]:::oplog
        Ops["Operations<br/>PK: (RepoId, OpId)"]:::oplog
        Views["Views<br/>PK: (RepoId, ViewId)"]:::oplog
        Repos -->|"INTERLEAVE"| Workspaces
    end

    subgraph ProjTables["Per-Project Object Tables (Top-Level)"]
        Projects["Projects<br/>PK: (ProjectId)"]:::cas
        Commits["Commits<br/>PK: (ProjectId, CommitId)"]:::cas
        Trees["Trees<br/>PK: (ProjectId, TreeId)"]:::cas
        TreeEntries["TreeEntries<br/>PK: (ProjectId, TreeId, Name)"]:::cas
        Symlinks["Symlinks<br/>PK: (ProjectId, SymlinkId)"]:::cas
        ProjBlobs["ProjectBlobs<br/>PK: (ProjectId, BlobId)"]:::cas
        Trees -->|"INTERLEAVE"| TreeEntries
    end

    Blobs["Blobs (Top-Level)<br/>PK: (BlobId)"]:::storage

    Repos -.->|"ProjectId"| Projects
    ProjBlobs -.->|"BlobId"| Blobs
```

* **Staged Wide-Directory Writes (`Trees.Ready`):** Cloud Spanner caps a single transaction at 80,000 cell mutations. Directories up to 4,000 entries write `Trees` (`Ready = TRUE`) and all `TreeEntries` in one atomic batch; larger directories stage `Ready = FALSE`, stream 4,000-entry batches, and flip `Ready = TRUE` on the final batch so readers never see partial directories.

### SQLite and In-Memory Metadata Stores

In Milestone 1, `SqliteMetadataStore` mirrors the same relational tables (`server/sql/sqlite_schema.sql`) in single-file `WAL` mode, pairing one serialized writer connection (`IMMEDIATE` transactions) with a pool of concurrent read-only `WAL` connections, while `MemoryMetadataStore` (evolving [MemoryStore](../server/src/store/memorystore.rs) on `main`) holds equivalent in-memory maps for fast unit tests.

### Cloud Storage and Local Content-Addressed Blobs

Both `GcsBlobStore` and `LocalFsBlobStore` in [server/src/store/](../server/src/store/) shard immutable file payloads under two-level hex prefixes (`blobs/<aa>/<bb>/<digest_hex>`). Small files below the inline threshold (for example, 16 KiB) live directly in `Blobs.InlineContent` with `PrimaryLocation = NULL`, while larger files stream from local disk via `Seek` or from Cloud Storage via HTTP `Range` requests with conditional write-once uploads and cross-region origin fallback in `ReplicatedBlobStore` (see [Cloud Spanner and Cloud Storage Architecture](spanner-and-gcs-design.md)).

### Workstation `.jj/` Layout

Because commits, trees, views, and operations live in Commit Cloud, a local workspace directory keeps minimal state on disk:

* **Backend Configuration (`.jj/repo/store/config.toml`):** Records `server_url` and `repo_id` (plus optional `project_id`) alongside `"commit_cloud"` type markers under `.jj/repo/`.
* **Local Commit Index (`.jj/repo/index/`):** Caches compact commit-graph segments synced from the server so local revset evaluation remains fast.
* **Working-Copy Stat Cache (`.jj/working_copy/`):** Tracks local file `stat` metadata for `LocalWorkingCopy` snapshot comparisons.
* **Local Daemon Cache (`$XDG_CACHE_HOME/jj-commit-cloud/`):** When jj-cc-daemon is active, immutable backend objects are cached on local disk alongside per-`(server, project_id)` reachability markers (so sibling checkouts in the same project share warm hits while unrelated projects stay isolated) and operation-log objects are partitioned by `(server, repo_id)`.

## Write-Before-Publish Ordering

Every mutation writes child objects before any parent object that references them:

$$\text{File / Symlink} \longrightarrow \text{Tree} \longrightarrow \text{Commit} \longrightarrow \text{View} \longrightarrow \text{Operation} \longrightarrow \text{OpHeads}$$

1. **Crash Safety Without Dangling Pointers:** File bytes finish uploading to `BlobStore` before their metadata record is published, child blobs and subtrees exist before a parent `Tree` is accepted, and `View` plus `Operation` rows exist before `OpHeads` advances. An interrupted client or server leaves only unreferenced content-addressed objects, never a broken repository head.
2. **Lock-Free Sibling Preservation:** Concurrent `UpdateOpHeads` calls advancing from base $[O_1]$ to $O_2$ and $O_3$ remove $O_1$ and preserve both $[O_2, O_3]$ inside serializable transactions, allowing subsequent loads to merge both branches without lost work.
