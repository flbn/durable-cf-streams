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
- uses `new_sqlite_classes` in wrangler migration (not `new_classes`)
- extends `DurableObject` base class for proper typing
