import { runConformanceTests } from "@durable-streams/server-conformance-tests";
import { DEFAULT_D1_MAX_CHUNK_BYTES } from "durable-cf-streams/storage/d1";
import { afterAll, beforeAll, describe, expect, it } from "vitest";
import { type Unstable_DevWorker, unstable_dev } from "wrangler";

let worker: Unstable_DevWorker;
const config = { baseUrl: "" };
const SQL_ROW_LIMIT_BYTES = 2_000_000;

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

describe("D1Store storage layout", () => {
  it("stores initial stream bodies beyond the SQL row limit in bounded chunks", async () => {
    const path = streamPath();
    const expected = "initial:".padEnd(SQL_ROW_LIMIT_BYTES + 300_000, "i");

    await createTextStream(path, expected);

    await expect(getText(path)).resolves.toBe(expected);
  });

  it("stores appended stream bodies beyond the SQL row limit", async () => {
    const path = streamPath();
    await createTextStream(path);

    let expected = "";
    while (expected.length <= SQL_ROW_LIMIT_BYTES) {
      const chunk = `${expected.length}:`.padEnd(300_000, "x");
      expected += chunk;
      await appendText(path, chunk);
    }

    await expect(getText(path)).resolves.toBe(expected);
  });

  it("rejects a single append above the chunk limit and keeps existing data", async () => {
    const path = streamPath();
    await createTextStream(path);
    await appendText(path, "ok");

    const response = await appendText(
      path,
      "x".repeat(DEFAULT_D1_MAX_CHUNK_BYTES + 1)
    );
    expect(response.status).toBe(413);
    await expect(response.text()).resolves.toContain("Payload too large");
    await expect(getText(path)).resolves.toBe("ok");
  });
});

async function createTextStream(path: string, body = ""): Promise<void> {
  const response = await fetch(`${config.baseUrl}${path}`, {
    method: "PUT",
    headers: { "Content-Type": "text/plain" },
    body,
  });
  expect(response.status).toBe(201);
}

async function appendText(path: string, body: string): Promise<Response> {
  const response = await fetch(`${config.baseUrl}${path}`, {
    method: "POST",
    headers: { "Content-Type": "text/plain" },
    body,
  });
  if (response.ok) {
    expect(response.status).toBe(204);
  }
  return response;
}

async function getText(path: string): Promise<string> {
  const response = await fetch(`${config.baseUrl}${path}`);
  expect(response.status).toBe(200);
  return await response.text();
}

function streamPath(): string {
  return `/storage-layout-${crypto.randomUUID()}`;
}
