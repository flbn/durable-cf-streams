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

this example uses `SqliteStore` because it is the smallest persistent setup. `SqliteStore` keeps each stream as one row, so it is a good fit when a stream is expected to stay below Cloudflare's 2 MB SQLite row and BLOB limit.

use `ChunkedSqliteStore` when a single stream can grow past that limit through many appends. it uses the same `StreamStore` interface as `SqliteStore`, so the Durable Object binding and request handlers can stay the same:

```typescript
import type { StreamStore } from "durable-cf-streams";
import { ChunkedSqliteStore } from "durable-cf-streams/storage/chunked-sqlite";

export class StreamDO extends DurableObject<Env> {
  private readonly store: StreamStore;

  constructor(state: DurableObjectState, env: Env) {
    super(state, env);
    const store = new ChunkedSqliteStore(state.storage.sql, {
      maxChunkBytes: 1_000_000,
    });
    store.initialize();
    this.store = store;
  }
}
```

existing `SqliteStore` bytes remain readable as a legacy prefix; enabling the chunked store does not rewrite old rows.

each individual append still has to fit within `maxChunkBytes`. if one event can exceed that size, split it before calling `append`.
