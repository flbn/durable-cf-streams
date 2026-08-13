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

this example uses `D1Store`, the persistent store for cloudflare d1. appends keep stream bytes and producer state in separate SQL rows so they do not rewrite growing SQL values.

## types

```bash
pnpm run types
```

generates `worker-configuration.d.ts` from `wrangler.toml`.
