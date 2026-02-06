import {
  DurableStreamHandler,
  type FetchHandlerOptions,
} from "durable-cf-streams";
import { MemoryStore } from "durable-cf-streams/storage/memory";
import { Router } from "itty-router";

type Env = {
  STREAMS: DurableObjectNamespace;
};

const router = Router();

router.all("*", async (request: Request, env: Env) => {
  const id = env.STREAMS.idFromName("global");
  const stub = env.STREAMS.get(id);
  try {
    return await stub.fetch(request);
  } catch (err) {
    console.error("Error forwarding request to DO:", err);
    return new Response("Bad Gateway", { status: 502 });
  }
});

export default {
  fetch: (request: Request, env: Env) => router.fetch(request, env),
};

export class StreamDO implements DurableObject {
  private readonly handler: DurableStreamHandler;

  constructor(_state: DurableObjectState, _env: Env) {
    const store = new MemoryStore();
    this.handler = new DurableStreamHandler({
      store,
      longPollTimeout: 5000,
    } as FetchHandlerOptions);
  }

  async fetch(request: Request): Promise<Response> {
    const url = new URL(request.url);
    this.handler.options.baseUrl = `${url.protocol}//${url.host}`;
    try {
      return await this.handler.fetch(request);
    } catch (err) {
      console.error("Error handling request in DO:", err);
      return new Response("Internal Server Error", { status: 500 });
    }
  }
}
