---
status: proposed
---

# Use an opt-in chunked SQLite layout for high-volume durable streams

durable-cf-streams will keep `SqliteStore` as the existing snapshot layout and add `ChunkedSqliteStore` as an opt-in `StreamStore` implementation for SQLite-backed Durable Objects. `ChunkedSqliteStore` stores stream metadata separately from bounded `stream_chunks` rows, allowing aggregate streams to grow beyond Cloudflare's per-row and per-BLOB SQL limit without moving chunking into consumers such as Nexus. Existing snapshot bytes in `streams.data` are treated as a legacy prefix; the first implementation reads them before chunk rows and appends new data after them, but does not silently replace `SqliteStore` or rewrite existing streams.

**Considered Options**

- Keep the snapshot layout and tell high-volume consumers to use KV or R2. Rejected because those adapters still rewrite one data key or object and inherit same-key or same-object write constraints.
- Make `SqliteStore` chunked by default immediately. Rejected for the first release because it silently changes a storage layout that existing users may depend on operationally.
- Add a chunked store as a caller-visible wrapper around existing stores. Rejected because chunking is a storage-layout concern and should not leak into product code.
- Add `ChunkedSqliteStore` as a sibling adapter with the same `StreamStore` interface. Accepted because it fixes the Cloudflare row-size limit at the adapter seam while preserving current conformance behavior and giving production consumers an explicit rollout path.

**Consequences**

- Aggregate streams can exceed 2 MB, but one prepared append still must fit inside the configured chunk limit and should throw `PayloadTooLargeError` when it does not.
- Full catch-up reads can still allocate large responses; read pagination or `maxReadBytes` may be a later extension.
- Waiter notification must be chunk-aware instead of assuming it can slice a complete in-memory snapshot.
- Forks should physically copy the requested prefix into the child stream in the first implementation; linked parent chunks are left for a later design.
- `SQLITE_TOOBIG`, `string or blob too big`, and equivalent D1 row-size failures should be normalized inside storage adapters as `PayloadTooLargeError`.
