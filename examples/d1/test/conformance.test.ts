import { runConformanceTests } from "@durable-streams/server-conformance-tests";

const baseUrl = process.env.CONFORMANCE_TEST_URL ?? "http://127.0.0.1:8787";

runConformanceTests({ baseUrl });
