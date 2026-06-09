# durable-cf-streams

building blocks for [durable streams](https://github.com/durable-streams/durable-streams) on cloudflare. storage backends and utilities. the idea is that you can borrow utilities and wire up http (or whatever) however you want.

## install

```bash
pnpm add durable-cf-streams
```

## storage backends

```typescript
import { MemoryStore } from "durable-cf-streams/storage/memory";
import { SqliteStore } from "durable-cf-streams/storage/sqlite";
import { D1Store } from "durable-cf-streams/storage/d1";
import { KVStore } from "durable-cf-streams/storage/kv";
import { R2Store } from "durable-cf-streams/storage/r2";

// in-memory (for durable objects without persistence)
const store = new MemoryStore();

// sqlite (for durable objects with persistence via SqlStorage)
const store = new SqliteStore(state.storage.sql);
store.initialize(); // creates table

// d1 database
const store = new D1Store(env.DB);
await store.initialize(); // creates table

// workers kv
const store = new KVStore(env.KV);

// r2 bucket
const store = new R2Store(env.BUCKET);
```

## streamstore interface

```typescript
interface StreamStore {
  put(path: string, options: PutOptions): Promise<PutResult>;
  append(path: string, data: Uint8Array, options?: AppendOptions): Promise<AppendResult>;
  get(path: string, options?: GetOptions): Promise<GetResult>;
  head(path: string): Promise<HeadResult | null>;
  delete(path: string): Promise<void>;
  has(path: string): boolean;
  waitForData(path: string, offset: string, timeoutMs: number): Promise<WaitResult>;
  formatResponse(path: string, messages: StreamMessage[]): Uint8Array;
}
```

## protocol constants

<!-- exported protocol constants from packages/durable-cf-streams/src/const.ts via packages/durable-cf-streams/src/index.ts -->

compatible with the [durable streams protocol](https://github.com/durable-streams/durable-streams):

```typescript
import {
  // header constants
  STREAM_OFFSET_HEADER,     // "Stream-Next-Offset"
  STREAM_CURSOR_HEADER,     // "Stream-Cursor"
  STREAM_UP_TO_DATE_HEADER, // "Stream-Up-To-Date"
  STREAM_SEQ_HEADER,        // "Stream-Seq"
  STREAM_TTL_HEADER,        // "Stream-TTL"
  STREAM_EXPIRES_AT_HEADER, // "Stream-Expires-At"
  STREAM_SSE_DATA_ENCODING_HEADER, // "Stream-SSE-Data-Encoding"
  STREAM_CLOSED_HEADER,     // "Stream-Closed"
  PRODUCER_ID_HEADER,       // "Producer-Id"
  PRODUCER_EPOCH_HEADER,    // "Producer-Epoch"
  PRODUCER_SEQ_HEADER,      // "Producer-Seq"
  PRODUCER_EXPECTED_SEQ_HEADER, // "Producer-Expected-Seq"
  PRODUCER_RECEIVED_SEQ_HEADER, // "Producer-Received-Seq"
  CACHE_CONTROL_HEADER,     // "Cache-Control"
  CONTENT_TYPE_OPTIONS_HEADER,        // "X-Content-Type-Options"
  CROSS_ORIGIN_RESOURCE_POLICY_HEADER, // "Cross-Origin-Resource-Policy"

  // response header values
  PROTOCOL_SECURITY_HEADERS,
  HEAD_CACHE_CONTROL_VALUE, // "no-store"
  SSE_CACHE_CONTROL_VALUE,  // "no-cache"

  // query param constants
  OFFSET_QUERY_PARAM,       // "offset"
  TAIL_OFFSET_QUERY_VALUE,  // "now"
  LIVE_QUERY_PARAM,         // "live"
  CURSOR_QUERY_PARAM,       // "cursor"

  // sse
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
  type Cursor,
  type ETag,
  type Offset,
  type ProducerState,
  type ProducerStateMap,
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
  StreamNotFoundError,
  isStreamError,
  streamErrorStatus,
} from "durable-cf-streams";

// check error type
if (error instanceof StreamNotFoundError) {
  return new Response("not found", { status: 404 });
}

// or map any known stream error to its protocol status
if (isStreamError(error)) {
  return new Response(error.message, { status: streamErrorStatus(error) });
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
    this.store = new SqliteStore(state.storage.sql);
    this.store.initialize();
  }

  async fetch(request: Request): Promise<Response> {
    const path = new URL(request.url).pathname;

    if (request.method === "PUT") {
      const contentType = request.headers.get("content-type") ?? "application/octet-stream";
      const body = new Uint8Array(await request.arrayBuffer());
      
      const result = await this.store.put(path, {
        contentType: normalizeContentType(contentType),
        data: body.length > 0 ? body : undefined,
      });

      return new Response(null, {
        status: result.created ? 201 : 200,
        headers: { [STREAM_OFFSET_HEADER]: result.nextOffset },
      });
    }

    // ...
  }
}
```

see [examples](../../examples) for complete implementations.

## license

mit
