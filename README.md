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
- [`sqlite`](examples/sqlite) - sqlite store (for durable objects with persistence)

## storage backends

- **MemoryStore** - in-memory, for durable objects without persistence
- **SqliteStore** - sqlite via `SqlStorage`, for durable objects with persistence
- **D1Store** - cloudflare d1 database
- **KVStore** - workers kv
- **R2Store** - r2 bucket

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
# 1. update conformance tests
pnpm update @durable-streams/server-conformance-tests -r --filter "./examples/*"

# 2. build and test
pnpm run build
pnpm run test

# 3. fix any failures, repeat
```

note: protocol constants and utilities are implemented locally for cloudflare workers compatibility (no node.js dependencies).

## releasing

release-please automatically creates a release PR when you merge to main. by default it bumps patch version based on conventional commits.

to override the version, edit the version in `packages/durable-cf-streams/package.json` directly in the release PR before merging.

when the release PR is merged, release-please creates a github release which triggers npm publish.

## license

mit
