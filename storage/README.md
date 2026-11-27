# cc-storage

Backend-neutral storage traits for the Commit Cloud server, reference
implementations, and conformance suites that any implementation can run.
Linear: DEV-191 (parent), DEV-192 (metadata store), DEV-193 (blob store),
DEV-194 (local-disk blob store); the suites seed DEV-93.

| Path | Contents |
|---|---|
| `src/metadata.rs` | `ObjectStore` and `MetadataStore` |
| `src/blob.rs` | `BlobStore` |
| `src/ids.rs`, `src/error.rs`, `src/digest.rs` | Shared names, errors and hashing |
| `src/memory.rs` | In-memory stores: reference behaviour and test doubles |
| `src/sqlite.rs` | Persistent SQLite metadata store for a single node |
| `src/local_disk.rs` | Local-disk blob store |
| `src/conformance/` | Suites any backend runs unchanged |
| `tests/` | The suites on every implementation here, plus restart tests |
| `examples/` | `write_loop` and `verify_all`: a kill -9 crash test for the local-disk store |

Features `conformance`, `local-disk` and `sqlite` are off by default, so a
dependent builds only what it asks for; the crate's own tests turn them on.
`rusqlite` is the same 0.31 the server uses.

## Decisions

Decided in review:

- Racing writers (DEV-147): `AdvanceMode` implements `Reject` and
  reserves `Merge`, which answers `Unsupported`.
- The closure check lives in the store, inside `advance_head`, and stores
  remember which objects have a verified closure.
- jj's root ids are virtual roots: treated as present, never stored.
- Retries: the store keeps the last `request_id` per workspace.
- Objects name their blobs in `blob_refs`; the store does not check that
  the blobs exist.
- `create_project` fails with `AlreadyExists` for an existing project.
- Workspace aliases are unique per server.
- Chunking (DEV-80, DEV-84): one blob per file in v0; a blob is the unit
  whether it later holds a whole file or one chunk. Large files are out
  of scope.
- Change notifications (DEV-143, DEV-145): the stores keep the event rows
  (`MetadataStore::read_events`); live push is not part of the traits.

Still open:

- Hash function and object encoding (DEV-79). `HashAlgorithm` is
  `non_exhaustive`, and the stores keep canonical bytes without parsing
  them.
- GCS object layout (DEV-189, DEV-190). It stays inside each `BlobStore`;
  `BlobKey` carries no object name or bucket.

## Next steps

Have `server` depend on this crate, then retire the `Store` trait in
`server/src/store/mod.rs` one backend at a time: memory first (DEV-44),
then Spanner (DEV-86, DEV-89).
