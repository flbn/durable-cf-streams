# durable-cf-streams

building blocks for [durable streams](https://github.com/durable-streams/durable-streams) on cloudflare. storage backends and utilities. the idea is that you can borrow utilities and wire up http (or whatever) however you want.

## install

```bash
pnpm add durable-cf-streams
```

## storage backends

<!-- storage backend exports from packages/durable-cf-streams/src/storage/index.ts and packages/durable-cf-streams/package.json#exports -->

```typescript
import { MemoryStore } from "durable-cf-streams/storage/memory";
import { SqliteStore } from "durable-cf-streams/storage/sqlite";
import { D1Store } from "durable-cf-streams/storage/d1";
import { KVStore } from "durable-cf-streams/storage/kv";
import { R2Store } from "durable-cf-streams/storage/r2";

// in-memory (for durable objects without persistence)
const store = new MemoryStore();

// sqlite (for durable objects with persistence and bounded chunk rows)
const store = new SqliteStore(state.storage);
store.initialize(); // creates tables

// d1 database with bounded chunk rows
const store = new D1Store(env.DB);
await store.initialize(); // creates tables

// workers kv
const store = new KVStore(env.KV, { serializedOwner: true });

// r2 bucket
const store = new R2Store(env.BUCKET, { serializedOwner: true });
```

`SqliteStore` and `D1Store` keep stream metadata, bytes, and producer state in separate SQL rows so a long stream does not rewrite one growing SQL value.
They use a breaking schema and do not migrate old `streams.data` rows.

NOTE: each create mints a fresh stream incarnation; send `Stream-If-Incarnation` when a client rejects stale reads or writes after delete and recreate of the same path.
NOTE: one logical put or append is capped at 12 MiB by default, then stored across bounded chunk rows when needed.
NOTE: SQL reads validate chunk indexes, counts, byte ranges, offsets, and data lengths before returning data.
NOTE: SQL reads return bounded windows; when `get()` returns `upToDate: false`, resume from the returned `nextOffset`.
NOTE: memory, KV, and R2 are whole-value stores; they cap the complete stream body with `maxStreamBytes` before they accept or materialize it.
NOTE: KV and R2 do not provide a compare-and-set append primitive, so callers route each stream path through one serialized owner and acknowledge that invariant with `{ serializedOwner: true }`.
NOTE: producer headers provide per-producer idempotency lanes; they do not acquire a single current writer claim for the whole stream.

## streamstore interface

```typescript
interface StreamStore {
  put(path: string, options: PutOptions): Promise<PutResult>;
  append(path: string, data: Uint8Array, options?: AppendOptions): Promise<AppendResult>;
  get(path: string, options?: GetOptions): Promise<GetResult>;
  head(path: string): Promise<HeadResult | null>;
  delete(path: string): Promise<void>;
  has(path: string): Promise<boolean>;
  waitForData(path: string, offset: string, timeoutMs: number, options?: WaitOptions): Promise<WaitResult>;
  formatResponse(path: string, messages: StreamMessage[]): Uint8Array;
}
```

## protocol constants

<!-- exported protocol constants from packages/durable-cf-streams/src/const.ts via packages/durable-cf-streams/src/index.ts -->

compatible with the [durable streams protocol](https://github.com/durable-streams/durable-streams):

```typescript
import {
  // header constants
  STREAM_OFFSET_HEADER,         // "Stream-Next-Offset"
  STREAM_CURSOR_HEADER,         // "Stream-Cursor"
  STREAM_UP_TO_DATE_HEADER,     // "Stream-Up-To-Date"
  STREAM_INCARNATION_HEADER,    // "Stream-Incarnation"
  STREAM_IF_INCARNATION_HEADER, // "Stream-If-Incarnation"
  STREAM_SEQ_HEADER,            // "Stream-Seq"
  STREAM_TTL_HEADER,            // "Stream-TTL"
  STREAM_EXPIRES_AT_HEADER,     // "Stream-Expires-At"
  STREAM_SSE_DATA_ENCODING_HEADER, // "Stream-SSE-Data-Encoding"
  STREAM_CLOSED_HEADER,         // "Stream-Closed"
  STREAM_FORKED_FROM_HEADER,    // "Stream-Forked-From"
  STREAM_FORK_OFFSET_HEADER,    // "Stream-Fork-Offset"
  STREAM_FORK_SUB_OFFSET_HEADER, // "Stream-Fork-Sub-Offset"
  RESERVED_CONTROL_PATH_SEGMENT, // "__ds"
  PRODUCER_ID_HEADER,           // "Producer-Id"
  PRODUCER_EPOCH_HEADER,        // "Producer-Epoch"
  PRODUCER_SEQ_HEADER,          // "Producer-Seq"
  PRODUCER_EXPECTED_SEQ_HEADER, // "Producer-Expected-Seq"
  PRODUCER_RECEIVED_SEQ_HEADER, // "Producer-Received-Seq"
  CACHE_CONTROL_HEADER,     // "Cache-Control"
  CONTENT_TYPE_OPTIONS_HEADER,        // "X-Content-Type-Options"
  CROSS_ORIGIN_RESOURCE_POLICY_HEADER, // "Cross-Origin-Resource-Policy"

  // response header values
  PROTOCOL_SECURITY_HEADERS,
  HEAD_CACHE_CONTROL_VALUE, // "no-store"
  SSE_CACHE_CONTROL_VALUE,  // "no-cache"
  DEFAULT_CONTENT_TYPE,     // "application/octet-stream"

  // query param constants
  OFFSET_QUERY_PARAM,       // "offset"
  TAIL_OFFSET_QUERY_VALUE,  // "now"
  LIVE_QUERY_PARAM,         // "live"
  CURSOR_QUERY_PARAM,       // "cursor"

  // sse
  SSE_OFFSET_FIELD, // "streamNextOffset"
  SSE_CURSOR_FIELD, // "streamCursor"
  SSE_CLOSED_FIELD, // "streamClosed"
  SSE_COMPATIBLE_CONTENT_TYPES,

  // path encoding
  encodeStreamPath,
  decodeStreamPath,

  // cursor utilities
  calculateCursor,
  generateResponseCursor,
  DEFAULT_CURSOR_EPOCH,
  DEFAULT_CURSOR_INTERVAL_SECONDS,
} from "durable-cf-streams";
```

## branded protocol types

<!-- exported branded protocol schemas and types from packages/durable-cf-streams/src/schema.ts via packages/durable-cf-streams/src/index.ts -->

```typescript
import {
  CursorSchema,
  ETagSchema,
  OffsetSchema,
  ProducerStateMapSchema,
  ProducerStateSchema,
  StreamIncarnationSchema,
  type Cursor,
  type ETag,
  type Offset,
  type ProducerState,
  type ProducerStateMap,
  type StreamIncarnation,
} from "durable-cf-streams";
```

## utilities

<!-- exported utility functions from packages/durable-cf-streams/src/index.ts -->

```typescript
import {
  // offsets
  parseOffset,
  formatOffset,
  compareOffsets,
  isValidOffset,
  initialOffset,
  isSentinelOffset,
  normalizeOffset,
  advanceOffset,
  incrementSeq,
  
  // protocol
  normalizeContentType,
  isJsonContentType,
  isSSETextCompatibleContentType,
  validateTTL,
  validateForkSubOffset,
  validateExpiresAt,
  generateETag,
  parseETag,
  processJsonAppend,
  formatJsonResponse,
  validateJsonCreate,
  encodeSSEData,
  encodeBase64Data,

  // producer idempotency
  parseProducerHeaders,
  evaluateProducerAppend,
  commitProducerAppend,
} from "durable-cf-streams";
```

## errors

<!-- exported error classes and helpers from packages/durable-cf-streams/src/errors.ts via packages/durable-cf-streams/src/index.ts -->

tagged errors for pattern matching:

```typescript
import {
  ContentTypeMismatchError,
  InvalidJsonError,
  InvalidOffsetError,
  InvalidProducerError,
  PayloadTooLargeError,
  ProducerFencedError,
  ProducerSequenceConflictError,
  SequenceConflictError,
  StreamClosedError,
  StreamConflictError,
  type StreamErrorEventData,
  StreamGoneError,
  StreamNotFoundError,
  streamErrorEventData,
  streamErrorEventJson,
  isStreamError,
  streamErrorHeaders,
  streamErrorStatus,
} from "durable-cf-streams";

// check error type
if (error instanceof StreamNotFoundError) {
  return new Response("not found", { status: 404 });
}

// or map any known stream error to its protocol status
if (isStreamError(error)) {
  return new Response(error.message, {
    headers: streamErrorHeaders(error),
    status: streamErrorStatus(error),
  });
}
```

## example

```typescript
import { SqliteStore } from "durable-cf-streams/storage/sqlite";
import {
  normalizeContentType,
  STREAM_OFFSET_HEADER,
} from "durable-cf-streams";

export class StreamDO extends DurableObject {
  private store: SqliteStore;

  constructor(state: DurableObjectState, env: Env) {
    super(state, env);
    this.store = new SqliteStore(state.storage);
    this.store.initialize();
  }

  async fetch(request: Request): Promise<Response> {
    const path = new URL(request.url).pathname;

    if (request.method === "PUT") {
      const contentType = request.headers.get("content-type");
      const body = new Uint8Array(await request.arrayBuffer());
      
      const result = await this.store.put(path, {
        contentType: contentType ? normalizeContentType(contentType) : undefined,
        data: body.length > 0 ? body : undefined,
      });

      return new Response(null, {
        status: result.created ? 201 : 200,
        headers: {
          [STREAM_INCARNATION_HEADER]: result.incarnation,
          [STREAM_OFFSET_HEADER]: result.nextOffset,
          "Content-Type": result.contentType,
        },
      });
    }

    // ...
  }
}
```

see [examples](../../examples) for complete implementations.

## license

mit
