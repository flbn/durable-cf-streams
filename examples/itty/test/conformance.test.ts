import { runConformanceTests } from "@durable-streams/server-conformance-tests";
import { unstable_dev, type UnstableDevWorker } from "wrangler";
import { beforeAll, afterAll } from "vitest";

let worker: UnstableDevWorker;
const config = { baseUrl: "" };

beforeAll(async () => {
	worker = await unstable_dev("src/index.ts", {
		experimental: { disableExperimentalWarning: true },
	});
	config.baseUrl = `http://${worker.address}:${worker.port}`;
});

afterAll(async () => {
	await worker?.stop();
});

runConformanceTests(config);
