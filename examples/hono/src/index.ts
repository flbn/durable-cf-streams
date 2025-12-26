import type { StreamStore } from "durable-cf-streams";
import {
  generateCursor,
  getNextCursor,
  normalizeContentType,
} from "durable-cf-streams";
import { MemoryStore } from "durable-cf-streams/storage/memory";
import { Hono } from "hono";
import {
  mapError,
  parseLiveMode,
  parseOffsetParam,
  parseTtlAndExpires,
} from "../../utils.js";

type Env = {
  STREAMS: DurableObjectNamespace;
};

const app = new Hono<{ Bindings: Env }>();

app.all("/*", (c) => {
  const id = c.env.STREAMS.idFromName("global");
  const stub = c.env.STREAMS.get(id);

  return stub.fetch(c.req.raw);
});

export default app;

export class StreamDO implements DurableObject {
  private readonly store: StreamStore;
  private readonly app: Hono;

  constructor(_state: DurableObjectState, _env: Env) {
    this.store = new MemoryStore();
    this.app = this.createApp();
  }

  private createApp(): Hono {
    const app = new Hono();

    app.put("*", async (c) => {
      const path = new URL(c.req.url).pathname;
      try {
        return await this.handlePut(path, c.req.raw);
      } catch (error) {
        return mapError(error);
      }
    });

    app.post("*", async (c) => {
      const path = new URL(c.req.url).pathname;
      try {
        return await this.handlePost(path, c.req.raw);
      } catch (error) {
        return mapError(error);
      }
    });

    app.get("*", async (c) => {
      const url = new URL(c.req.url);
      const path = url.pathname;
      try {
        return await this.handleGet(path, url, c.req.raw);
      } catch (error) {
        return mapError(error);
      }
    });

    app.on("HEAD", "*", async (c) => {
      const path = new URL(c.req.url).pathname;
      try {
        return await this.handleHead(path);
      } catch (error) {
        return mapError(error);
      }
    });

    app.delete("*", async (c) => {
      const path = new URL(c.req.url).pathname;
      try {
        return await this.handleDelete(path);
      } catch (error) {
        return mapError(error);
      }
    });

    app.all("*", () => new Response("Method Not Allowed", { status: 405 }));

    return app;
  }

  async fetch(request: Request): Promise<Response> {
    return await this.app.fetch(request);
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
      "Stream-Next-Offset": result.nextOffset,
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
      request.headers.get("stream-seq") ??
      request.headers.get("x-seq") ??
      undefined;

    const result = await this.store.append(path, data, {
      contentType: normalizeContentType(contentType),
      seq,
    });

    return new Response(null, {
      status: 200,
      headers: {
        "Stream-Next-Offset": result.nextOffset,
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
          "Stream-Next-Offset": result.nextOffset,
          "Stream-Cursor": result.cursor,
          "Stream-Up-To-Date": "true",
        },
      });
    }

    const body = this.store.formatResponse(path, result.messages);

    return new Response(body, {
      status: 200,
      headers: {
        "Content-Type": result.contentType,
        ETag: result.etag,
        "Stream-Next-Offset": result.nextOffset,
        "Stream-Cursor": result.cursor,
        "Stream-Up-To-Date": result.upToDate ? "true" : "false",
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
        "Stream-Cursor": generateCursor(),
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
      const cursor = getNextCursor(clientCursor);
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
          "Stream-Next-Offset": initial.nextOffset,
          "Stream-Cursor": getNextCursor(clientCursor),
          "Stream-Up-To-Date": "true",
        },
      });
    }

    if (ifNoneMatch && initial.etag === ifNoneMatch) {
      return new Response(null, {
        status: 304,
        headers: {
          ETag: initial.etag,
          "Stream-Next-Offset": initial.nextOffset,
          "Stream-Cursor": getNextCursor(clientCursor),
          "Stream-Up-To-Date": "true",
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
          "Stream-Next-Offset": current.nextOffset,
          "Stream-Cursor": getNextCursor(clientCursor),
          "Stream-Up-To-Date": "true",
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
        "Stream-Next-Offset": result.nextOffset,
        "Stream-Cursor": getNextCursor(clientCursor),
        "Stream-Up-To-Date": "true",
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
        "Stream-Next-Offset": result.nextOffset,
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
