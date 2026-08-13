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

this example uses `SqliteStore`, the persistent store for sqlite-backed durable objects. appends keep stream bytes and producer state in separate SQL rows so they do not rewrite growing SQL values.
