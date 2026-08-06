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

## production store choice

<!-- production sqlite store guidance from packages/durable-cf-streams/src/storage/sqlite.ts and packages/durable-cf-streams/src/storage/chunked-sqlite.ts -->

this example uses `SqliteStore`, the default persistent store for SQLite-backed Durable Objects. `SqliteStore` uses the chunked layout, so one stream can grow past Cloudflare's 2 MB SQLite row and BLOB limit through many appends.

set `maxChunkBytes` when one appended event needs a smaller per-row ceiling than the default. the Durable Object binding and request handlers stay the same:

```typescript
import type { StreamStore } from "durable-cf-streams";
import { SqliteStore } from "durable-cf-streams/storage/sqlite";

export class StreamDO extends DurableObject<Env> {
  private readonly store: StreamStore;

  constructor(state: DurableObjectState, env: Env) {
    super(state, env);
    const store = new SqliteStore(state.storage.sql, {
      maxChunkBytes: 1_000_000,
    });
    store.initialize();
    this.store = store;
  }
}
```

existing snapshot bytes remain readable as a legacy prefix; enabling the current layout does not rewrite old rows.

each individual append still has to fit within `maxChunkBytes`. if one event can exceed that size, split it before calling `append`.
