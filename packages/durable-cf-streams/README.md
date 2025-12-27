# durable-cf-streams

building blocks for [durable streams](https://github.com/durable-streams/durable-streams) on cloudflare. storage backends and utilities. the idea is that you can borrow utilities and wire up http (or whatever) however you want.

## install

```bash
pnpm add durable-cf-streams
```

## storage backends

```typescript
import { MemoryStore } from "durable-cf-streams/storage/memory";
import { D1Store } from "durable-cf-streams/storage/d1";
import { KVStore } from "durable-cf-streams/storage/kv";
import { R2Store } from "durable-cf-streams/storage/r2";

// in-memory (for durable objects)
const store = new MemoryStore();

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

  // query param constants
  OFFSET_QUERY_PARAM,       // "offset"
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

## utilities

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
  validateTTL,
  validateExpiresAt,
  generateETag,
  parseETag,
  processJsonAppend,
  formatJsonResponse,
  validateJsonCreate,
} from "durable-cf-streams";
```

## errors

tagged errors for pattern matching:

```typescript
import {
  StreamNotFoundError,
  SequenceConflictError,
  ContentTypeMismatchError,
  StreamConflictError,
  InvalidJsonError,
  InvalidOffsetError,
  PayloadTooLargeError,
} from "durable-cf-streams";

// check error type
if (error instanceof StreamNotFoundError) {
  return new Response("not found", { status: 404 });
}

// or use _tag for switch
switch (error._tag) {
  case "StreamNotFoundError": return new Response("not found", { status: 404 });
  case "SequenceConflictError": return new Response("conflict", { status: 409 });
}
```

## example

```typescript
import { MemoryStore } from "durable-cf-streams/storage/memory";
import {
  normalizeContentType,
  STREAM_OFFSET_HEADER,
} from "durable-cf-streams";

export class StreamDO implements DurableObject {
  private store = new MemoryStore();

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
