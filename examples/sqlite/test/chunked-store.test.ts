import { afterAll, beforeAll, describe, expect, it } from "vitest";
import { type Unstable_DevWorker, unstable_dev } from "wrangler";

type Command = Record<string, unknown> & { op: string };

type CommandResult<T> =
  | { ok: true; status: number; body: T }
  | { ok: false; status: number; body: { error: StreamErrorBody } };

type StreamErrorBody = {
  tag: string;
  message: string;
  maxBytes?: number;
  receivedBytes?: number;
};

type ChunkStats = {
  maxChunkBytes: number;
  legacyBytes: number;
  chunkBytes: number;
  maxStoredChunkBytes: number;
};

let worker: Unstable_DevWorker;
const config = { baseUrl: "" };

beforeAll(async () => {
  worker = await unstable_dev("test/chunked-worker.ts", {
    experimental: { disableExperimentalWarning: true },
    local: true,
    persist: false,
  });
  config.baseUrl = `http://${worker.address}:${worker.port}`;
});

afterAll(async () => {
  await worker?.stop();
});

describe("ChunkedSqliteStore", () => {
  it("stores aggregate streams beyond the SQLite row limit in bounded chunks", async () => {
    const path = streamPath();
    await expectOk(command({ op: "put", path, contentType: "text/plain" }));

    let expected = "";
    for (let i = 0; i < 9; i++) {
      const data = `${i}:`.padEnd(300_000, "x");
      expected += data;
      await expectOk(
        command({ op: "append", path, contentType: "text/plain", data })
      );
    }

    const read = await expectOk<{ body: string }>(command({ op: "get", path }));
    expect(read.body).toBe(expected);

    const stats = await getStats(path);
    expect(stats.legacyBytes).toBe(0);
    expect(stats.chunkBytes).toBeGreaterThan(2_000_000);
    expect(stats.maxStoredChunkBytes).toBeLessThanOrEqual(stats.maxChunkBytes);
  });

  it("reads from chunk boundaries and from inside a chunk", async () => {
    const path = streamPath();
    await expectOk(command({ op: "put", path, contentType: "text/plain" }));
    await expectOk(
      command({
        op: "append",
        path,
        contentType: "text/plain",
        data: "a".repeat(10),
      })
    );
    await expectOk(
      command({
        op: "append",
        path,
        contentType: "text/plain",
        data: "b".repeat(12),
      })
    );
    await expectOk(
      command({
        op: "append",
        path,
        contentType: "text/plain",
        data: "c".repeat(8),
      })
    );

    const fromBoundary = await expectOk<{ body: string }>(
      command({ op: "get", path, offset: offset(1, 10) })
    );
    expect(fromBoundary.body).toBe(`${"b".repeat(12)}${"c".repeat(8)}`);

    const fromInside = await expectOk<{ body: string }>(
      command({ op: "get", path, offset: offset(1, 15) })
    );
    expect(fromInside.body).toBe(`${"b".repeat(7)}${"c".repeat(8)}`);
  });

  it("formats JSON streams correctly across chunk rows", async () => {
    const path = streamPath();
    await expectOk(
      command({ op: "put", path, contentType: "application/json" })
    );

    for (let i = 0; i < 35; i++) {
      await expectOk(
        command({
          op: "append",
          path,
          contentType: "application/json",
          data: JSON.stringify({ i, body: "x".repeat(70_000) }),
        })
      );
    }

    const read = await expectOk<{ body: string }>(command({ op: "get", path }));
    const parsed = JSON.parse(read.body) as { i: number; body: string }[];
    expect(parsed).toHaveLength(35);
    expect(parsed[34]).toMatchObject({ i: 34 });
    expect(parsed[34]?.body).toHaveLength(70_000);
  });

  it("wakes waiters at the tail without materializing a snapshot", async () => {
    const path = streamPath();
    await expectOk(command({ op: "put", path, contentType: "text/plain" }));
    const head = await expectOk<{ nextOffset: string }>(
      command({ op: "head", path })
    );

    const wait = command<{ body: string; timedOut: boolean }>({
      op: "wait",
      path,
      offset: head.nextOffset,
      timeoutMs: 5000,
    });
    await new Promise((resolve) => setTimeout(resolve, 50));
    await expectOk(
      command({ op: "append", path, contentType: "text/plain", data: "hello" })
    );

    const result = await expectOk(wait);
    expect(result.timedOut).toBe(false);
    expect(result.body).toBe("hello");
  });

  it("rejects an oversized single append and preserves existing chunks", async () => {
    const path = streamPath();
    await expectOk(command({ op: "put", path, contentType: "text/plain" }));
    await expectOk(
      command({ op: "append", path, contentType: "text/plain", data: "ok" })
    );

    const stats = await getStats(path);
    const rejected = await command({
      op: "append",
      path,
      contentType: "text/plain",
      data: "x".repeat(stats.maxChunkBytes + 1),
    });

    expect(rejected.ok).toBe(false);
    if (!rejected.ok) {
      expect(rejected.status).toBe(413);
      expect(rejected.body.error.tag).toBe("PayloadTooLargeError");
      expect(rejected.body.error.maxBytes).toBe(stats.maxChunkBytes);
      expect(rejected.body.error.receivedBytes).toBe(stats.maxChunkBytes + 1);
    }

    const read = await expectOk<{ body: string }>(command({ op: "get", path }));
    expect(read.body).toBe("ok");
  });

  it("normalizes snapshot SqliteStore row-size failures", async () => {
    const rejected = await command({
      op: "snapshotTooLarge",
      path: streamPath(),
      size: 2_000_001,
    });

    expect(rejected.ok).toBe(false);
    if (!rejected.ok) {
      expect(rejected.status).toBe(413);
      expect(rejected.body.error.tag).toBe("PayloadTooLargeError");
      expect(rejected.body.error.maxBytes).toBe(2_000_000);
      expect(rejected.body.error.receivedBytes).toBe(2_000_001);
    }
  });

  it("treats old snapshot bytes as a legacy prefix", async () => {
    const path = streamPath();
    await expectOk(
      command({
        op: "seedLegacy",
        path,
        contentType: "text/plain",
        data: "legacy-",
      })
    );
    await expectOk(
      command({ op: "append", path, contentType: "text/plain", data: "chunk" })
    );

    const read = await expectOk<{ body: string }>(command({ op: "get", path }));
    expect(read.body).toBe("legacy-chunk");
  });

  it("does not create duplicate chunks for duplicate producer appends", async () => {
    const path = streamPath();
    const producer = { id: "producer-a", epoch: 1, seq: 0 };
    await expectOk(command({ op: "put", path, contentType: "text/plain" }));

    const first = await expectOk<{ appended?: boolean }>(
      command({
        op: "append",
        path,
        contentType: "text/plain",
        data: "once",
        producer,
      })
    );
    const second = await expectOk<{
      appended?: boolean;
      producer?: { duplicate: boolean };
    }>(
      command({
        op: "append",
        path,
        contentType: "text/plain",
        data: "once",
        producer,
      })
    );

    expect(first.appended).toBe(true);
    expect(second.appended).toBe(false);
    expect(second.producer?.duplicate).toBe(true);
    const read = await expectOk<{ body: string }>(command({ op: "get", path }));
    expect(read.body).toBe("once");
  });

  it("keeps forks readable after the source stream is deleted", async () => {
    const source = streamPath();
    const child = streamPath();
    await expectOk(
      command({ op: "put", path: source, contentType: "text/plain" })
    );
    await expectOk(
      command({
        op: "append",
        path: source,
        contentType: "text/plain",
        data: "abc",
      })
    );
    await expectOk(
      command({
        op: "append",
        path: source,
        contentType: "text/plain",
        data: "def",
      })
    );

    await expectOk(
      command({
        op: "put",
        path: child,
        contentType: "text/plain",
        forkedFrom: source,
      })
    );
    await expectOk(
      command({
        op: "append",
        path: child,
        contentType: "text/plain",
        data: "ghi",
      })
    );
    await expectOk(command({ op: "delete", path: source }));

    const read = await expectOk<{ body: string }>(
      command({ op: "get", path: child })
    );
    expect(read.body).toBe("abcdefghi");
  });
});

async function getStats(path: string): Promise<ChunkStats> {
  return await expectOk<ChunkStats>(command({ op: "stats", path }));
}

async function expectOk<T>(result: Promise<CommandResult<T>>): Promise<T> {
  const response = await result;
  expect(response.ok).toBe(true);
  if (!response.ok) {
    throw new Error(response.body.error.message);
  }
  return response.body;
}

async function command<T = unknown>(body: Command): Promise<CommandResult<T>> {
  const response = await fetch(config.baseUrl, {
    method: "POST",
    body: JSON.stringify(body),
  });
  const responseBody = (await response.json()) as
    | T
    | { error: StreamErrorBody };
  return response.ok
    ? { ok: true, status: response.status, body: responseBody as T }
    : {
        ok: false,
        status: response.status,
        body: responseBody as { error: StreamErrorBody },
      };
}

function streamPath(): string {
  return `/chunked-${crypto.randomUUID()}`;
}

function offset(seq: number, pos: number): string {
  return `${seq.toString(16).padStart(16, "0")}_${pos
    .toString(16)
    .padStart(16, "0")}`;
}
