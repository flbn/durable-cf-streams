---
status: proposed
---

# Use a chunked SQLite layout for high-volume durable streams

durable-cf-streams first added `ChunkedSqliteStore` as an opt-in `StreamStore` implementation for SQLite-backed Durable Objects, then made that layout the `SqliteStore` default in the breaking storage slice. `SqliteStore` stores stream metadata separately from bounded `stream_chunks` rows, allowing aggregate streams to grow beyond Cloudflare's per-row and per-BLOB SQL limit without moving chunking into consumers such as Nexus. Existing snapshot bytes in `streams.data` are treated as a legacy prefix; the implementation reads them before chunk rows and appends new data after them, but does not rewrite existing streams.

`ChunkedD1Store` uses the same layout for D1 because D1 has the same row and BLOB limit with an async prepared-statement API. It is a sibling store rather than a shared base class so each adapter stays close to the Cloudflare API it wraps.

**Considered Options**

- Keep the snapshot layout and tell high-volume consumers to use KV or R2. Rejected because those adapters still rewrite one data key or object and inherit same-key or same-object write constraints.
- Make `SqliteStore` chunked by default immediately. Rejected for the first slice because it silently changes a storage layout that existing users may depend on operationally.
- Add a chunked store as a caller-visible wrapper around existing stores. Rejected because chunking is a storage-layout concern and should not leak into product code.
- Add `ChunkedSqliteStore` and `ChunkedD1Store` as sibling adapters with the same `StreamStore` interface. Accepted because it fixes the Cloudflare row-size limit at the adapter seam while preserving current conformance behavior and giving production consumers an explicit rollout path.
- Make `SqliteStore` use the chunked layout after the opt-in slices. Accepted for the breaking storage slice because the default store should no longer fail high-volume append streams at the snapshot row boundary.

**Consequences**

- Aggregate streams can exceed 2 MB, but one prepared append still must fit inside the configured chunk limit and should throw `PayloadTooLargeError` when it does not.
- Full catch-up reads can still allocate large responses; read pagination or `maxReadBytes` may be a later extension.
- Waiter notification must be chunk-aware instead of assuming it can slice a complete in-memory snapshot.
- Forks should physically copy the requested prefix into the child stream in the first implementation; linked parent chunks are left for a later design.
- `SQLITE_TOOBIG`, `string or blob too big`, and equivalent D1 row-size failures should be normalized inside storage adapters as `PayloadTooLargeError`.
- This is a breaking default-layout change for `SqliteStore`; `ChunkedSqliteStore` remains as an explicit import name for the same SQLite layout.
