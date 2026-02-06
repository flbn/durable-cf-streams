import {
	DurableStreamHandler,
	type FetchHandlerOptions,
} from "durable-cf-streams";
import { MemoryStore } from "durable-cf-streams/storage/memory";
import { Hono } from "hono";

type Env = {
	STREAMS: DurableObjectNamespace;
};

const app = new Hono<{ Bindings: Env }>();

app.all("/*", async (c) => {
	const id = c.env.STREAMS.idFromName("global");
	const stub = c.env.STREAMS.get(id);
	try {
		return await stub.fetch(c.req.raw);
	} catch (err) {
		return c.text("Bad Gateway", 502);
	}
});

export default app;

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
			console.error(err);
			return new Response("Internal Server Error", { status: 500 });
		}
	}
}
