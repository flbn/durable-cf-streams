# sqlite example

durable streams implementation using `SqliteStore` with Durable Objects' `SqlStorage` for persistence.

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

<!-- sqlite store behavior from packages/durable-cf-streams/src/storage/sqlite.ts and packages/durable-cf-streams/src/storage/chunked-sqlite.ts -->

this example uses `SqliteStore`, the default persistent store for sqlite-backed durable objects. appends use bounded chunk rows by default, and existing snapshot bytes remain readable as a legacy prefix.
