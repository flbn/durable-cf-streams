import type { StreamStore } from "durable-cf-streams";
import {
  calculateCursor,
  generateResponseCursor,
  normalizeContentType,
  STREAM_CURSOR_HEADER,
  STREAM_OFFSET_HEADER,
  STREAM_SEQ_HEADER,
  STREAM_UP_TO_DATE_HEADER,
} from "durable-cf-streams";
import { KVStore } from "durable-cf-streams/storage/kv";
import {
  mapError,
  parseLiveMode,
  parseOffsetParam,
  parseTtlAndExpires,
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

  constructor(_state: DurableObjectState, env: Env) {
    this.store = new KVStore(env.KV);
  }

  async fetch(request: Request): Promise<Response> {
    const url = new URL(request.url);
    const path = url.pathname;

    try {
      switch (request.method) {
        case "PUT":
          return await this.handlePut(path, request);
        case "POST":
          return await this.handlePost(path, request);
        case "GET":
          return await this.handleGet(path, url, request);
        case "HEAD":
          return await this.handleHead(path);
        case "DELETE":
          return await this.handleDelete(path);
        default:
          return new Response("Method Not Allowed", { status: 405 });
      }
    } catch (error) {
      return mapError(error);
    }
  }

  private async handlePut(path: string, request: Request): Promise<Response> {
    const contentType =
      request.headers.get("content-type") ?? "application/octet-stream";

    const ttlResult = parseTtlAndExpires(request);
    if (!ttlResult.ok) {
      return ttlResult.error;
    }
    const { ttlSeconds, expiresAt } = ttlResult;

    const body = await request.arrayBuffer();
    const data = new Uint8Array(body);

    const result = await this.store.put(path, {
      contentType: normalizeContentType(contentType),
      ttlSeconds,
      expiresAt,
      data: data.length > 0 ? data : undefined,
    });

    const status = result.created ? 201 : 200;
    const headers: Record<string, string> = {
      [STREAM_OFFSET_HEADER]: result.nextOffset,
      "Content-Type": normalizeContentType(contentType),
    };
    if (result.created) {
      headers.Location = request.url.split("?")[0] ?? request.url;
    }
    return new Response(null, { status, headers });
  }

  private async handlePost(path: string, request: Request): Promise<Response> {
    const contentType = request.headers.get("content-type");
    if (!contentType) {
      return new Response("Content-Type header required", { status: 400 });
    }

    if (!this.store.has(path)) {
      return new Response(`Stream not found: ${path}`, { status: 404 });
    }

    const body = await request.arrayBuffer();
    const data = new Uint8Array(body);

    if (data.length === 0) {
      return new Response("Empty body not allowed", { status: 400 });
    }

    const seq =
      request.headers.get(STREAM_SEQ_HEADER) ??
      request.headers.get("x-seq") ??
      undefined;

    const result = await this.store.append(path, data, {
      contentType: normalizeContentType(contentType),
      seq,
    });

    return new Response(null, {
      status: 200,
      headers: {
        [STREAM_OFFSET_HEADER]: result.nextOffset,
      },
    });
  }

  private async handleGet(
    path: string,
    url: URL,
    request: Request
  ): Promise<Response> {
    const cursorParam = url.searchParams.get("cursor");
    const ifNoneMatch = request.headers.get("if-none-match");

    const offsetResult = parseOffsetParam(url.searchParams.get("offset"));
    if (!offsetResult.ok) {
      return offsetResult.error;
    }
    const { offset } = offsetResult;

    const liveMode = parseLiveMode(url, request, offset);
    if (liveMode.mode === "error") {
      return liveMode.error;
    }

    if (liveMode.mode === "sse") {
      return this.handleSSE(path, offset as string, cursorParam ?? undefined);
    }
    if (liveMode.mode === "long-poll") {
      return await this.handleLongPoll(
        path,
        offset as string,
        cursorParam ?? undefined,
        ifNoneMatch ?? undefined
      );
    }

    return await this.handleSimpleGet(path, offset, ifNoneMatch);
  }

  private async handleSimpleGet(
    path: string,
    offset: string | undefined,
    ifNoneMatch: string | null
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
      },
    });
  }

  private handleSSE(
    path: string,
    offset: string,
    clientCursor?: string
  ): Response {
    const state = { currentOffset: offset, cancelled: false };

    const stream = new ReadableStream({
      start: (controller) => {
        this.runSSELoop(path, state, clientCursor, controller);
      },
      cancel: () => {
        state.cancelled = true;
      },
    });

    return new Response(stream, {
      status: 200,
      headers: {
        "Content-Type": "text/event-stream",
        "Cache-Control": "no-cache",
        Connection: "keep-alive",
        [STREAM_CURSOR_HEADER]: calculateCursor(),
      },
    });
  }

  private async runSSELoop(
    path: string,
    state: { currentOffset: string; cancelled: boolean },
    clientCursor: string | undefined,
    controller: ReadableStreamDefaultController<Uint8Array>
  ): Promise<void> {
    const encoder = new TextEncoder();

    const send = (event: string, data: string) => {
      controller.enqueue(encoder.encode(`event: ${event}\ndata: ${data}\n\n`));
    };

    const sendControl = (nextOffset: string) => {
      const cursor = generateResponseCursor(clientCursor);
      send(
        "control",
        JSON.stringify({
          streamCursor: cursor,
          streamNextOffset: nextOffset,
          upToDate: true,
        })
      );
    };

    const sendData = (data: string, contentType: string) => {
      if (contentType.includes("json")) {
        send("data", data);
      } else {
        const lines = data
          .split("\n")
          .map((line) => `data: ${line}`)
          .join("\n");
        controller.enqueue(encoder.encode(`event: data\n${lines}\n\n`));
      }
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
    state: { currentOffset: string; cancelled: boolean },
    sendControl: (offset: string) => void,
    sendData: (data: string, contentType: string) => void
  ): Promise<void> {
    if (!this.store.has(path)) {
      throw new Error("Stream not found");
    }

    const initial = await this.store.get(path, { offset: state.currentOffset });
    if (initial.messages.length > 0) {
      const body = this.store.formatResponse(path, initial.messages);
      sendData(new TextDecoder().decode(body), initial.contentType);
      state.currentOffset = initial.nextOffset;
    }
    sendControl(state.currentOffset);

    while (!state.cancelled) {
      if (!this.store.has(path)) {
        throw new Error("Stream not found");
      }

      const wait = await this.store.waitForData(
        path,
        state.currentOffset,
        30_000
      );

      if (wait.timedOut) {
        sendControl(state.currentOffset);
        continue;
      }

      if (wait.messages.length > 0) {
        const result = await this.store.get(path, {
          offset: state.currentOffset,
        });
        const body = this.store.formatResponse(path, result.messages);
        sendData(new TextDecoder().decode(body), result.contentType);
        state.currentOffset = result.nextOffset;
        sendControl(state.currentOffset);
      }
    }
  }

  private async handleLongPoll(
    path: string,
    offset: string,
    clientCursor?: string,
    ifNoneMatch?: string
  ): Promise<Response> {
    if (!this.store.has(path)) {
      return new Response(`Stream not found: ${path}`, { status: 404 });
    }

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
        },
      });
    }

    const wait = await this.store.waitForData(path, offset, 30_000);

    if (wait.timedOut) {
      const current = await this.store.get(path, { offset });
      const body = this.store.formatResponse(path, current.messages);
      return new Response(body, {
        status: 200,
        headers: {
          "Content-Type": current.contentType,
          ETag: current.etag,
          [STREAM_OFFSET_HEADER]: current.nextOffset,
          [STREAM_CURSOR_HEADER]: generateResponseCursor(clientCursor),
          [STREAM_UP_TO_DATE_HEADER]: "true",
        },
      });
    }

    const result = await this.store.get(path, { offset });
    const body = this.store.formatResponse(path, result.messages);

    return new Response(body, {
      status: 200,
      headers: {
        "Content-Type": result.contentType,
        ETag: result.etag,
        [STREAM_OFFSET_HEADER]: result.nextOffset,
        [STREAM_CURSOR_HEADER]: generateResponseCursor(clientCursor),
        [STREAM_UP_TO_DATE_HEADER]: "true",
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
        ETag: result.etag,
        [STREAM_OFFSET_HEADER]: result.nextOffset,
      },
    });
  }

  private async handleDelete(path: string): Promise<Response> {
    if (!this.store.has(path)) {
      return new Response(`Stream not found: ${path}`, { status: 404 });
    }

    await this.store.delete(path);

    return new Response(null, { status: 204 });
  }
}
