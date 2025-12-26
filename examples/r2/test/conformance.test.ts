import { runConformanceTests } from "@durable-streams/server-conformance-tests";
import { afterAll, beforeAll } from "vitest";
import { type Unstable_DevWorker, unstable_dev } from "wrangler";

let worker: Unstable_DevWorker;
const config = { baseUrl: "" };

beforeAll(async () => {
	worker = await unstable_dev("src/index.ts", {
		experimental: { disableExperimentalWarning: true },
		local: true,
		persist: false,
	});
	config.baseUrl = `http://${worker.address}:${worker.port}`;
});

afterAll(async () => {
  await worker?.stop();
});

runConformanceTests(config);
