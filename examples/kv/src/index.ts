import type { Offset, StreamStore } from "durable-cf-streams";
import {
  CACHE_CONTROL_HEADER,
  calculateCursor,
  encodeBase64Data,
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
  streamErrorEventJson,
} from "durable-cf-streams";
import { KVStore } from "durable-cf-streams/storage/kv";
import {
  appendResponse,
  createAsyncQueue,
  createSSEWriter,
  handleLongPollResponse,
  isReservedControlPath,
  isStreamClosedRequest,
  LIVE_WAIT_TIMEOUT_MS,
  mapError,
  parseExpectedIncarnation,
  parseForkOptions,
  parseProducerOptions,
  parsePutContentType,
  parseTtlAndExpires,
  pumpSSEStream,
  reservedControlResponse,
  resolveReadRequest,
  type SSEDataEncoding,
  streamMetadataHeaders,
  tailOffsetCacheHeaders,
  withProtocolHeaders,
} from "../../utils.js";

type Env = {
  STREAMS: DurableObjectNamespace;
  KV: KVNamespace;
};

export default {
  fetch(request: Request, env: Env): Promise<Response> {
    const id = env.STREAMS.idFromName("global");
    const stub = env.STREAMS.get(id);

    return stub.fetch(request);
  },
};

export class StreamDO implements DurableObject {
  private readonly store: StreamStore;
  private readonly mutationQueue = createAsyncQueue();

  constructor(_state: DurableObjectState, env: Env) {
    this.store = new KVStore(env.KV, { serializedOwner: true });
  }

  async fetch(request: Request): Promise<Response> {
    const url = new URL(request.url);
    const path = url.pathname;

    try {
      if (isReservedControlPath(path)) {
        return withProtocolHeaders(reservedControlResponse());
      }

      switch (request.method) {
        case "PUT":
          return withProtocolHeaders(await this.handlePut(path, request));
        case "POST":
          return withProtocolHeaders(await this.handlePost(path, request));
        case "GET":
          return withProtocolHeaders(await this.handleGet(path, url, request));
        case "HEAD":
          return withProtocolHeaders(await this.handleHead(path));
        case "DELETE":
          return withProtocolHeaders(await this.handleDelete(path));
        default:
          return withProtocolHeaders(
            new Response("Method Not Allowed", { status: 405 })
          );
      }
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

    const result = await this.mutationQueue(() =>
      this.store.put(path, {
        contentType,
        expectedIncarnation: parseExpectedIncarnation(request),
        ttlSeconds,
        expiresAt,
        data: data.length > 0 ? data : undefined,
        closed: isStreamClosedRequest(request),
        forkedFrom: forkResult.forkedFrom,
        forkOffset: forkResult.forkOffset,
        forkSubOffset: forkResult.forkSubOffset,
      })
    );

    const status = result.created ? 201 : 200;
    const headers: Record<string, string> = {
      [STREAM_OFFSET_HEADER]: result.nextOffset,
      "Content-Type": result.contentType,
      ...streamMetadataHeaders(result),
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

    const result = await this.mutationQueue(() =>
      this.store.append(path, data, {
        contentType:
          data.length > 0 && contentType
            ? normalizeContentType(contentType)
            : undefined,
        close,
        expectedIncarnation: parseExpectedIncarnation(request),
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
    const expectedIncarnation = parseExpectedIncarnation(request);

    if (liveMode.mode === "sse" && offset !== undefined) {
      return this.handleSSE(
        path,
        offset,
        cursorParam ?? undefined,
        liveMode.encoding,
        expectedIncarnation
      );
    }
    if (liveMode.mode === "long-poll" && offset !== undefined) {
      return await this.handleLongPoll(
        path,
        offset,
        cursorParam ?? undefined,
        ifNoneMatch ?? undefined,
        expectedIncarnation
      );
    }

    return await this.mutationQueue(() =>
      this.handleSimpleGet(
        path,
        offset,
        ifNoneMatch,
        isTail,
        expectedIncarnation
      )
    );
  }

  private async handleSimpleGet(
    path: string,
    offset: Offset | undefined,
    ifNoneMatch: string | null,
    isTailOffset: boolean,
    expectedIncarnation: ReturnType<typeof parseExpectedIncarnation>
  ): Promise<Response> {
    const result = await this.store.get(path, { offset, expectedIncarnation });

    if (ifNoneMatch && result.etag === ifNoneMatch) {
      return new Response(null, {
        status: 304,
        headers: {
          ETag: result.etag,
          [STREAM_OFFSET_HEADER]: result.nextOffset,
          [STREAM_CURSOR_HEADER]: result.cursor,
          [STREAM_UP_TO_DATE_HEADER]: result.upToDate ? "true" : "false",
          ...streamMetadataHeaders(result),
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
        ...streamMetadataHeaders(result),
        ...tailOffsetCacheHeaders(isTailOffset),
      },
    });
  }

  private handleSSE(
    path: string,
    offset: Offset,
    clientCursor?: string,
    encoding?: SSEDataEncoding,
    expectedIncarnation?: ReturnType<typeof parseExpectedIncarnation>
  ): Response {
    const state = {
      currentOffset: offset,
      cancelled: false,
      expectedIncarnation,
    };

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
    state: {
      currentOffset: Offset;
      cancelled: boolean;
      expectedIncarnation?: ReturnType<typeof parseExpectedIncarnation>;
    },
    clientCursor: string | undefined,
    encoding: SSEDataEncoding | undefined,
    controller: ReadableStreamDefaultController<Uint8Array>
  ): Promise<void> {
    const sse = createSSEWriter(controller);

    const sendControl = (
      nextOffset: Offset,
      closed = false,
      upToDate = true
    ) => {
      const cursor = generateResponseCursor(clientCursor);
      sse.send(
        "control",
        JSON.stringify(
          closed
            ? {
                [SSE_OFFSET_FIELD]: nextOffset,
                upToDate,
                [SSE_CLOSED_FIELD]: true,
              }
            : {
                [SSE_CURSOR_FIELD]: cursor,
                [SSE_OFFSET_FIELD]: nextOffset,
                upToDate,
              }
        )
      );
    };

    const sendData = (data: Uint8Array, _contentType: string) => {
      sse.send(
        "data",
        encoding === "base64"
          ? encodeBase64Data(data)
          : new TextDecoder().decode(data)
      );
    };

    const heartbeat = setInterval(() => {
      if (!state.cancelled) {
        sse.comment("heartbeat");
      }
    }, 15_000);

    try {
      await this.processSSEStream(path, state, sendControl, sendData);
    } catch (error) {
      if (!state.cancelled) {
        sse.send("error", streamErrorEventJson(error));
      }
    } finally {
      clearInterval(heartbeat);
      sse.flush();
      controller.close();
    }
  }

  private async processSSEStream(
    path: string,
    state: {
      currentOffset: Offset;
      cancelled: boolean;
      expectedIncarnation?: ReturnType<typeof parseExpectedIncarnation>;
    },
    sendControl: (offset: Offset, closed?: boolean, upToDate?: boolean) => void,
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
    ifNoneMatch?: string,
    expectedIncarnation?: ReturnType<typeof parseExpectedIncarnation>
  ): Promise<Response> {
    return await handleLongPollResponse(
      this.store,
      path,
      offset,
      clientCursor,
      ifNoneMatch,
      expectedIncarnation
    );
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
    return await this.mutationQueue(async () => {
      const head = await this.store.head(path);
      if (!head) {
        return new Response(`Stream not found: ${path}`, { status: 404 });
      }

      await this.store.delete(path);

      return new Response(null, { status: 204 });
    });
  }
}
