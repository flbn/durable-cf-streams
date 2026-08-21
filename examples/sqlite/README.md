# sqlite example

durable streams implementation using `SqliteStore` with durable object storage for persistence.

## run tests

```bash
pnpm install
pnpm test
```

## key differences from memory example

- uses `SqliteStore` instead of `MemoryStore`
- data persists across DO restarts
- declares the Durable Object with `new_sqlite_classes` in `wrangler.toml`
- extends `DurableObject` base class for proper typing

## storage notes

<!-- sqlite store behavior from packages/durable-cf-streams/src/storage/sqlite.ts -->

this example uses `SqliteStore`, the persistent store for sqlite-backed durable objects. it keeps stream bytes and producer state in separate SQL rows so appends do not rewrite one growing value.

NOTE: writes use Durable Object SQLite transactions, so stream metadata and chunk rows commit together.
NOTE: each create mints a fresh stream incarnation; send `Stream-If-Incarnation` when a client rejects stale reads or writes after delete and recreate of the same path.
NOTE: one logical put or append is capped at 12 MiB by default, then stored across bounded chunk rows when needed.
NOTE: reads validate chunk indexes, counts, byte ranges, offsets, and data lengths before returning data.
NOTE: reads return bounded windows; keep reading from `Stream-Next-Offset` until `Stream-Up-To-Date` is `true`.
