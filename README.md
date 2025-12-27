# durable-cf-streams

pure building blocks for durable streams on cloudflare. no framework, no opinions on http or runtime.

## packages

- [`durable-cf-streams`](packages/durable-cf-streams) - core library with storage backends and utilities

## examples

each example passes the full 124-test conformance suite:

- [`d1`](examples/d1) - d1 database backend
- [`hono`](examples/hono) - hono router with memory store
- [`itty`](examples/itty) - itty-router with memory store
- [`kv`](examples/kv) - workers kv backend
- [`memory`](examples/memory) - in-memory store (for durable objects)
- [`r2`](examples/r2) - r2 bucket backend

## quick start

```bash
pnpm install
pnpm run build
pnpm run test
```

## development

```bash
pnpm run lint        # run biome
pnpm run lint:fix    # fix lint issues
pnpm run check       # typecheck
pnpm run build       # build all packages
pnpm run test        # run all tests
```

## bumping durable-streams

when the upstream protocol changes:

```bash
# 1. update all durable-streams packages
pnpm update @durable-streams/client @durable-streams/server -r
pnpm update @durable-streams/server-conformance-tests -r --filter "./examples/*"

# 2. build and test
pnpm run build
pnpm run test

# 3. fix any failures, repeat
```

## license

mit
