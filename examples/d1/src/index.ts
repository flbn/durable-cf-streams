import {
	DurableStreamHandler,
	type FetchHandlerOptions,
	PayloadTooLargeError,
} from "durable-cf-streams";
import { D1Store } from "durable-cf-streams/storage/d1";

type Env = {
	STREAMS: DurableObjectNamespace;
	DB: D1Database;
};

export default {
	async fetch(request: Request, env: Env): Promise<Response> {
		const id = env.STREAMS.idFromName("global");
		const stub = env.STREAMS.get(id);
		try {
			return await stub.fetch(request);
		} catch (err) {
			const url = new URL(request.url);
			console.error(`[WORKER ERROR] ${request.method} ${url.pathname}${url.search} =>`, err);
			return new Response(
				`DO error: ${err instanceof Error ? err.message : String(err)}`,
				{ status: 502 },
			);
		}
	},
};

export class StreamDO implements DurableObject {
	private readonly handler: DurableStreamHandler;
	private initialized = false;
	private readonly store: D1Store;

	constructor(_state: DurableObjectState, env: Env) {
		this.store = new D1Store(env.DB);
		this.handler = new DurableStreamHandler({
			store: this.store,
			longPollTimeout: 5_000,
		} as FetchHandlerOptions);
	}

	async fetch(request: Request): Promise<Response> {
		try {
			if (!this.initialized) {
				await this.store.initialize();
				this.initialized = true;
			}
			const url = new URL(request.url);
			this.handler.options.baseUrl = `${url.protocol}//${url.host}`;
			return await this.handler.fetch(request);
		} catch (err) {
			if (err instanceof PayloadTooLargeError) {
				return new Response("Payload too large", { status: 413 });
			}
			console.error(`[DO CRASH] ${request.method} ${new URL(request.url).pathname}`, err);
			return new Response(
				`Internal error: ${err instanceof Error ? err.message : String(err)}`,
				{ status: 500 },
			);
		}
	}
}
