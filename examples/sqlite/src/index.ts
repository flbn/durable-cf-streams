import { DurableObject } from "cloudflare:workers";
import {
  DurableStreamHandler,
  type FetchHandlerOptions,
  PayloadTooLargeError,
} from "durable-cf-streams";
import { SqliteStore } from "durable-cf-streams/storage/sqlite";

type Env = {
  STREAMS: DurableObjectNamespace<StreamDO>;
};

export default {
  async fetch(request: Request, env: Env): Promise<Response> {
    const id = env.STREAMS.idFromName("global");
    const stub = env.STREAMS.get(id);
    try {
      return await stub.fetch(request);
    } catch (err) {
      const url = new URL(request.url);
      console.error(
        `[WORKER ERROR] ${request.method} ${url.pathname}${url.search} =>`,
        err
      );
      return new Response(
        `DO error: ${err instanceof Error ? err.message : String(err)}`,
        { status: 502 }
      );
    }
  },
};

export class StreamDO extends DurableObject<Env> {
  private readonly handler: DurableStreamHandler;

  constructor(state: DurableObjectState, env: Env) {
    super(state, env);
    const store = new SqliteStore(state.storage.sql);
    store.initialize();
    this.handler = new DurableStreamHandler({
      store,
      longPollTimeout: 5000,
    } as FetchHandlerOptions);
  }

  async fetch(request: Request): Promise<Response> {
    try {
      const url = new URL(request.url);
      this.handler.options.baseUrl = `${url.protocol}//${url.host}`;
      return await this.handler.fetch(request);
    } catch (err) {
      if (err instanceof PayloadTooLargeError) {
        return new Response("Payload too large", { status: 413 });
      }
      console.error(
        `[DO CRASH] ${request.method} ${new URL(request.url).pathname}`,
        err
      );
      return new Response(
        `Internal error: ${err instanceof Error ? err.message : String(err)}`,
        { status: 500 }
      );
    }
  }
}
