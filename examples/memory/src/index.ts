import {
	DurableStreamHandler,
	type FetchHandlerOptions,
} from "durable-cf-streams";
import { MemoryStore } from "durable-cf-streams/storage/memory";

type Env = {
	STREAMS: DurableObjectNamespace;
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

	constructor(_state: DurableObjectState, _env: Env) {
		const store = new MemoryStore();
		this.handler = new DurableStreamHandler({
			store,
			longPollTimeout: 5_000,
		} as FetchHandlerOptions);
	}

	async fetch(request: Request): Promise<Response> {
		try {
			const url = new URL(request.url);
			this.handler.options.baseUrl = `${url.protocol}//${url.host}`;
			return await this.handler.fetch(request);
		} catch (err) {
			console.error(`[DO CRASH] ${request.method} ${new URL(request.url).pathname}`, err);
			return new Response(
				`Internal error: ${err instanceof Error ? err.message : String(err)}`,
				{ status: 500 },
			);
		}
	}
}
