# r2 example

durable streams with cloudflare r2 bucket storage.

## run

```bash
pnpm run dev
# or with custom port
pnpm run dev --port 8706
```

## test

```bash
pnpm run test
# or against custom port
CONFORMANCE_TEST_URL=http://127.0.0.1:8706 pnpm run test
```

runs the [durable-streams conformance suite](https://github.com/durable-streams/durable-streams/tree/main/packages/server-conformance-tests).

## types

```bash
pnpm run types
```

generates `worker-configuration.d.ts` from `wrangler.toml`.
