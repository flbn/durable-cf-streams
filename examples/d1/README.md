# d1 example

durable streams with cloudflare d1 (sqlite) storage.

## run

```bash
pnpm run dev
# or with custom port
pnpm run dev --port 8701
```

## test

```bash
pnpm run test
# or against custom port
CONFORMANCE_TEST_URL=http://127.0.0.1:8701 pnpm run test
```

runs the [durable-streams conformance suite](https://github.com/durable-streams/durable-streams/tree/main/packages/server-conformance-tests).

## storage notes

<!-- d1 store behavior from packages/durable-cf-streams/src/storage/d1.ts -->

this example uses `D1Store`, the persistent store for cloudflare d1. it keeps stream bytes and producer state in separate SQL rows so appends do not rewrite one growing value.

NOTE: writes normally go through one serialized owner, like the Durable Object in this example. `D1Store` still fences write mutations against the stream metadata row so accidental concurrent writers fail with a conflict instead of racing silently.
NOTE: each create mints a fresh stream incarnation; send `Stream-If-Incarnation` when a client rejects stale reads or writes after delete and recreate of the same path.
NOTE: one logical put or append is capped at 12 MiB by default, then stored across bounded chunk rows when needed.
NOTE: reads validate chunk indexes, counts, byte ranges, offsets, and data lengths before returning data.
NOTE: reads return bounded windows; keep reading from `Stream-Next-Offset` until `Stream-Up-To-Date` is `true`.

## types

```bash
pnpm run types
```

generates `worker-configuration.d.ts` from `wrangler.toml`.
