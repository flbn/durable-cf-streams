import type { Offset, StreamStore } from "durable-cf-streams";
import {
  CACHE_CONTROL_HEADER,
  calculateCursor,
  encodeBase64Data,
  encodeSSEData,
  generateResponseCursor,
  HEAD_CACHE_CONTROL_VALUE,
  normalizeContentType,
  SSE_CACHE_CONTROL_VALUE,
  SSE_CLOSED_FIELD,
  SSE_CURSOR_FIELD,
  SSE_OFFSET_FIELD,
  STREAM_CURSOR_HEADER,
  STREAM_OFFSET_HEADER,
  STREAM_SEQ_HEADER,
  STREAM_SSE_DATA_ENCODING_HEADER,
  STREAM_UP_TO_DATE_HEADER,
} from "durable-cf-streams";
import { MemoryStore } from "durable-cf-streams/storage/memory";
import { Hono } from "hono";
import {
  appendResponse,
  createAsyncQueue,
  isReservedControlPath,
  isStreamClosedRequest,
  LIVE_WAIT_TIMEOUT_MS,
  mapError,
  parseForkOptions,
  parseProducerOptions,
  parsePutContentType,
  parseTtlAndExpires,
  pumpSSEStream,
  reservedControlResponse,
  resolveReadRequest,
  type SSEDataEncoding,
  streamClosedHeaders,
  streamMetadataHeaders,
  tailOffsetCacheHeaders,
  withProtocolHeaders,
} from "../../utils.js";

type Env = {
  STREAMS: DurableObjectNamespace;
};

const app = new Hono<{ Bindings: Env }>();

function getStreamStub(env: Env): DurableObjectStub {
  const id = env.STREAMS.idFromName("global");
  return env.STREAMS.get(id);
}

app.all("/*", (c) => getStreamStub(c.env).fetch(c.req.raw));

export default {
  async fetch(
    request: Request,
    env: Env,
    ctx: ExecutionContext
  ): Promise<Response> {
    if (request.method === "HEAD") {
      return getStreamStub(env).fetch(request);
    }

    return await app.fetch(request, env, ctx);
  },
};

export class StreamDO implements DurableObject {
  private readonly store: StreamStore;
  private readonly appendQueue = createAsyncQueue();
  private readonly app: Hono;

  constructor(_state: DurableObjectState, _env: Env) {
    this.store = new MemoryStore();
    this.app = this.createApp();
  }

  private createApp(): Hono {
    const app = new Hono();

    app.onError((error) => mapError(error));

    app.put("*", async (c) => {
      const path = new URL(c.req.url).pathname;
      return await this.handlePut(path, c.req.raw);
    });

    app.post("*", async (c) => {
      const path = new URL(c.req.url).pathname;
      return await this.handlePost(path, c.req.raw);
    });

    app.get("*", async (c) => {
      const url = new URL(c.req.url);
      const path = url.pathname;
      return await this.handleGet(path, url, c.req.raw);
    });

    app.delete("*", async (c) => {
      const path = new URL(c.req.url).pathname;
      return await this.handleDelete(path);
    });

    app.all("*", () => new Response("Method Not Allowed", { status: 405 }));

    return app;
  }

  async fetch(request: Request): Promise<Response> {
    try {
      const path = new URL(request.url).pathname;
      if (isReservedControlPath(path)) {
        return withProtocolHeaders(reservedControlResponse());
      }

      if (request.method === "HEAD") {
        return withProtocolHeaders(await this.handleHead(path));
      }

      return withProtocolHeaders(await this.app.fetch(request));
    } catch (error) {
      return withProtocolHeaders(mapError(error));
    }
  }

  private async handlePut(path: string, request: Request): Promise<Response> {
    const ttlResult = parseTtlAndExpires(request);
    if (!ttlResult.ok) {
      return ttlResult.error;
    }
    const { ttlSeconds, expiresAt } = ttlResult;
    const forkResult = parseForkOptions(request);
    if (!forkResult.ok) {
      return forkResult.error;
    }
    const contentType = parsePutContentType(request, forkResult.forkedFrom);

    const body = await request.arrayBuffer();
    const data = new Uint8Array(body);

    const result = await this.store.put(path, {
      contentType,
      ttlSeconds,
      expiresAt,
      data: data.length > 0 ? data : undefined,
      closed: isStreamClosedRequest(request),
      forkedFrom: forkResult.forkedFrom,
      forkOffset: forkResult.forkOffset,
    });

    const status = result.created ? 201 : 200;
    const headers: Record<string, string> = {
      [STREAM_OFFSET_HEADER]: result.nextOffset,
      "Content-Type": result.contentType,
      ...streamClosedHeaders(result.closed),
    };
    if (result.created) {
      headers.Location = request.url.split("?")[0];
    }
    return new Response(null, { status, headers });
  }

  private async handlePost(path: string, request: Request): Promise<Response> {
    const contentType = request.headers.get("content-type");
    const body = await request.arrayBuffer();
    const data = new Uint8Array(body);
    const close = isStreamClosedRequest(request);

    if (data.length > 0 && !contentType) {
      return new Response("Content-Type header required", { status: 400 });
    }

    if (data.length === 0 && !close) {
      return new Response("Empty body not allowed", { status: 400 });
    }

    const seq = request.headers.get(STREAM_SEQ_HEADER) ?? undefined;
    const producer = parseProducerOptions(request);

    const result = await this.appendQueue(() =>
      this.store.append(path, data, {
        contentType:
          data.length > 0 && contentType
            ? normalizeContentType(contentType)
            : undefined,
        close,
        producer,
        seq,
      })
    );

    return appendResponse(result);
  }

  private async handleGet(
    path: string,
    url: URL,
    request: Request
  ): Promise<Response> {
    const cursorParam = url.searchParams.get("cursor");
    const ifNoneMatch = request.headers.get("if-none-match");

    const readRequest = await resolveReadRequest(
      this.store,
      path,
      url,
      request
    );
    if (!readRequest.ok) {
      return readRequest.error;
    }
    const { offset, isTail, liveMode } = readRequest;

    if (liveMode.mode === "sse" && offset !== undefined) {
      return this.handleSSE(
        path,
        offset,
        cursorParam ?? undefined,
        liveMode.encoding
      );
    }
    if (liveMode.mode === "long-poll" && offset !== undefined) {
      return await this.handleLongPoll(
        path,
        offset,
        cursorParam ?? undefined,
        ifNoneMatch ?? undefined
      );
    }

    return await this.handleSimpleGet(path, offset, ifNoneMatch, isTail);
  }

  private async handleSimpleGet(
    path: string,
    offset: Offset | undefined,
    ifNoneMatch: string | null,
    isTailOffset: boolean
  ): Promise<Response> {
    const result = await this.store.get(path, { offset });

    if (ifNoneMatch && result.etag === ifNoneMatch) {
      return new Response(null, {
        status: 304,
        headers: {
          ETag: result.etag,
          [STREAM_OFFSET_HEADER]: result.nextOffset,
          [STREAM_CURSOR_HEADER]: result.cursor,
          [STREAM_UP_TO_DATE_HEADER]: "true",
          ...streamClosedHeaders(result.closed),
          ...tailOffsetCacheHeaders(isTailOffset),
        },
      });
    }

    const body = this.store.formatResponse(path, result.messages);

    return new Response(body, {
      status: 200,
      headers: {
        "Content-Type": result.contentType,
        ETag: result.etag,
        [STREAM_OFFSET_HEADER]: result.nextOffset,
        [STREAM_CURSOR_HEADER]: result.cursor,
        [STREAM_UP_TO_DATE_HEADER]: result.upToDate ? "true" : "false",
        ...streamClosedHeaders(result.closed),
        ...tailOffsetCacheHeaders(isTailOffset),
      },
    });
  }

  private handleSSE(
    path: string,
    offset: Offset,
    clientCursor?: string,
    encoding?: SSEDataEncoding
  ): Response {
    const state = { currentOffset: offset, cancelled: false };

    const stream = new ReadableStream({
      start: (controller) => {
        this.runSSELoop(path, state, clientCursor, encoding, controller);
      },
      cancel: () => {
        state.cancelled = true;
      },
    });

    return new Response(stream, {
      status: 200,
      headers: {
        "Content-Type": "text/event-stream",
        [CACHE_CONTROL_HEADER]: SSE_CACHE_CONTROL_VALUE,
        Connection: "keep-alive",
        [STREAM_CURSOR_HEADER]: calculateCursor(),
        ...(encoding === "base64"
          ? { [STREAM_SSE_DATA_ENCODING_HEADER]: encoding }
          : {}),
      },
    });
  }

  private async runSSELoop(
    path: string,
    state: { currentOffset: Offset; cancelled: boolean },
    clientCursor: string | undefined,
    encoding: SSEDataEncoding | undefined,
    controller: ReadableStreamDefaultController<Uint8Array>
  ): Promise<void> {
    const encoder = new TextEncoder();

    const send = (event: string, data: string) => {
      controller.enqueue(
        encoder.encode(`event: ${event}\n${encodeSSEData(data)}\n\n`)
      );
    };

    const sendControl = (nextOffset: Offset, closed = false) => {
      const cursor = generateResponseCursor(clientCursor);
      send(
        "control",
        JSON.stringify(
          closed
            ? {
                [SSE_OFFSET_FIELD]: nextOffset,
                upToDate: true,
                [SSE_CLOSED_FIELD]: true,
              }
            : {
                [SSE_CURSOR_FIELD]: cursor,
                [SSE_OFFSET_FIELD]: nextOffset,
                upToDate: true,
              }
        )
      );
    };

    const sendData = (data: Uint8Array, _contentType: string) => {
      send(
        "data",
        encoding === "base64"
          ? encodeBase64Data(data)
          : new TextDecoder().decode(data)
      );
    };

    const heartbeat = setInterval(() => {
      if (!state.cancelled) {
        controller.enqueue(encoder.encode(": heartbeat\n\n"));
      }
    }, 15_000);

    try {
      await this.processSSEStream(path, state, sendControl, sendData);
    } catch (error) {
      if (!state.cancelled) {
        const message =
          error instanceof Error ? error.message : "Unknown error";
        send("error", JSON.stringify({ error: message }));
      }
    } finally {
      clearInterval(heartbeat);
      controller.close();
    }
  }

  private async processSSEStream(
    path: string,
    state: { currentOffset: Offset; cancelled: boolean },
    sendControl: (offset: Offset, closed?: boolean) => void,
    sendData: (data: Uint8Array, contentType: string) => void
  ): Promise<void> {
    await pumpSSEStream(
      this.store,
      path,
      state,
      LIVE_WAIT_TIMEOUT_MS,
      sendControl,
      sendData
    );
  }

  private async handleLongPoll(
    path: string,
    offset: Offset,
    clientCursor?: string,
    ifNoneMatch?: string
  ): Promise<Response> {
    const initial = await this.store.get(path, { offset });

    if (initial.messages.length > 0) {
      const body = this.store.formatResponse(path, initial.messages);
      return new Response(body, {
        status: 200,
        headers: {
          "Content-Type": initial.contentType,
          ETag: initial.etag,
          [STREAM_OFFSET_HEADER]: initial.nextOffset,
          [STREAM_CURSOR_HEADER]: generateResponseCursor(clientCursor),
          [STREAM_UP_TO_DATE_HEADER]: "true",
          ...streamClosedHeaders(initial.closed),
        },
      });
    }

    if (ifNoneMatch && initial.etag === ifNoneMatch) {
      return new Response(null, {
        status: 304,
        headers: {
          ETag: initial.etag,
          [STREAM_OFFSET_HEADER]: initial.nextOffset,
          [STREAM_CURSOR_HEADER]: generateResponseCursor(clientCursor),
          [STREAM_UP_TO_DATE_HEADER]: "true",
          ...streamClosedHeaders(initial.closed),
        },
      });
    }

    if (initial.closed) {
      return new Response(null, {
        status: 204,
        headers: {
          ETag: initial.etag,
          [STREAM_OFFSET_HEADER]: initial.nextOffset,
          [STREAM_CURSOR_HEADER]: generateResponseCursor(clientCursor),
          [STREAM_UP_TO_DATE_HEADER]: "true",
          ...streamClosedHeaders(initial.closed),
        },
      });
    }

    const wait = await this.store.waitForData(
      path,
      offset,
      LIVE_WAIT_TIMEOUT_MS
    );

    if (wait.timedOut) {
      const current = await this.store.get(path, { offset });
      if (current.messages.length > 0) {
        const body = this.store.formatResponse(path, current.messages);
        return new Response(body, {
          status: 200,
          headers: {
            "Content-Type": current.contentType,
            ETag: current.etag,
            [STREAM_OFFSET_HEADER]: current.nextOffset,
            [STREAM_CURSOR_HEADER]: generateResponseCursor(clientCursor),
            [STREAM_UP_TO_DATE_HEADER]: "true",
            ...streamClosedHeaders(current.closed),
          },
        });
      }

      return new Response(null, {
        status: 204,
        headers: {
          ETag: current.etag,
          [STREAM_OFFSET_HEADER]: current.nextOffset,
          [STREAM_CURSOR_HEADER]: generateResponseCursor(clientCursor),
          [STREAM_UP_TO_DATE_HEADER]: "true",
          ...streamClosedHeaders(current.closed),
        },
      });
    }

    const result = await this.store.get(path, { offset });
    if (result.messages.length === 0) {
      return new Response(null, {
        status: 204,
        headers: {
          ETag: result.etag,
          [STREAM_OFFSET_HEADER]: result.nextOffset,
          [STREAM_CURSOR_HEADER]: generateResponseCursor(clientCursor),
          [STREAM_UP_TO_DATE_HEADER]: "true",
          ...streamClosedHeaders(result.closed),
        },
      });
    }
    const body = this.store.formatResponse(path, result.messages);

    return new Response(body, {
      status: 200,
      headers: {
        "Content-Type": result.contentType,
        ETag: result.etag,
        [STREAM_OFFSET_HEADER]: result.nextOffset,
        [STREAM_CURSOR_HEADER]: generateResponseCursor(clientCursor),
        [STREAM_UP_TO_DATE_HEADER]: "true",
        ...streamClosedHeaders(result.closed),
      },
    });
  }

  private async handleHead(path: string): Promise<Response> {
    const result = await this.store.head(path);

    if (!result) {
      return new Response(null, { status: 404 });
    }

    return new Response(null, {
      status: 200,
      headers: {
        "Content-Type": result.contentType,
        [CACHE_CONTROL_HEADER]: HEAD_CACHE_CONTROL_VALUE,
        ETag: result.etag,
        [STREAM_OFFSET_HEADER]: result.nextOffset,
        ...streamMetadataHeaders(result),
      },
    });
  }

  private async handleDelete(path: string): Promise<Response> {
    const head = await this.store.head(path);
    if (!head) {
      return new Response(`Stream not found: ${path}`, { status: 404 });
    }

    await this.store.delete(path);

    return new Response(null, { status: 204 });
  }
}
