# hono example

durable streams with [hono](https://hono.dev) router and in-memory storage.

## run

```bash
pnpm run dev
# or with custom port
pnpm run dev --port 8702
```

## test

```bash
pnpm run test
# or against custom port
CONFORMANCE_TEST_URL=http://127.0.0.1:8702 pnpm run test
```

runs the [durable-streams conformance suite](https://github.com/durable-streams/durable-streams/tree/main/packages/server-conformance-tests).

## types

```bash
pnpm run types
```

generates `worker-configuration.d.ts` from `wrangler.toml`.
