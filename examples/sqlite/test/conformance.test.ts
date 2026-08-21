import { runConformanceTests } from "@durable-streams/server-conformance-tests";
import {
  DEFAULT_SQLITE_MAX_APPEND_BYTES,
  DEFAULT_SQLITE_MAX_CHUNK_BYTES,
  DEFAULT_SQLITE_MAX_READ_BYTES,
} from "durable-cf-streams/storage/sqlite";
import { afterAll, beforeAll } from "vitest";
import { type Unstable_DevWorker, unstable_dev } from "wrangler";
import { runSqlStreamLimitTests } from "../../sql-stream-limit-tests";

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
runSqlStreamLimitTests({
  name: "SqliteStore",
  config,
  maxAppendBytes: DEFAULT_SQLITE_MAX_APPEND_BYTES,
  maxChunkBytes: DEFAULT_SQLITE_MAX_CHUNK_BYTES,
  maxReadBytes: DEFAULT_SQLITE_MAX_READ_BYTES,
});
