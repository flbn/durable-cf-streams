import {
  PRODUCER_EPOCH_HEADER,
  PRODUCER_ID_HEADER,
  PRODUCER_SEQ_HEADER,
  STREAM_CLOSED_HEADER,
  STREAM_FORK_OFFSET_HEADER,
  STREAM_FORK_SUB_OFFSET_HEADER,
  STREAM_FORKED_FROM_HEADER,
  STREAM_IF_INCARNATION_HEADER,
  STREAM_INCARNATION_HEADER,
  STREAM_OFFSET_HEADER,
  STREAM_UP_TO_DATE_HEADER,
} from "durable-cf-streams";
import { describe, expect, it } from "vitest";

type SqlStreamLimitTestOptions = {
  readonly name: string;
  readonly config: { readonly baseUrl: string };
  readonly maxAppendBytes: number;
  readonly maxChunkBytes: number;
  readonly maxReadBytes: number;
};

const SQL_ROW_LIMIT_BYTES = 2_000_000;
const INITIAL_OFFSET = "0000000000000000_0000000000000000";

export function runSqlStreamLimitTests(
  options: SqlStreamLimitTestOptions
): void {
  describe(`${options.name} SQL stream limits`, () => {
    it("returns bounded read windows that resume from Stream-Next-Offset", async () => {
      const path = streamPath();
      const expected = "read-window:".padEnd(options.maxReadBytes + 50, "r");

      await createTextStream(options, path, expected);

      const response = await fetch(`${options.config.baseUrl}${path}`);
      expect(response.status).toBe(200);
      expect(response.headers.get(STREAM_UP_TO_DATE_HEADER)).toBe("false");
      expect(response.headers.get(STREAM_OFFSET_HEADER)).toBeTruthy();
      await expect(response.text()).resolves.toBe(
        expected.slice(0, options.maxReadBytes)
      );
      await expect(getText(options, path)).resolves.toBe(expected);
    });

    it("keeps long-poll freshness false when the first window is partial", async () => {
      const path = streamPath();
      const expected = "long-poll-window:".padEnd(
        options.maxReadBytes + 50,
        "l"
      );

      await createTextStream(options, path, expected);

      const response = await fetch(
        `${options.config.baseUrl}${path}?live=long-poll&offset=${INITIAL_OFFSET}`
      );
      expect(response.status).toBe(200);
      expect(response.headers.get(STREAM_UP_TO_DATE_HEADER)).toBe("false");
      await expect(response.text()).resolves.toBe(
        expected.slice(0, options.maxReadBytes)
      );
    });

    it("accepts create bodies larger than one SQL value", async () => {
      const path = streamPath();
      const expected = "initial:".padEnd(SQL_ROW_LIMIT_BYTES + 300_000, "i");

      await createTextStream(options, path, expected);

      await expect(getText(options, path)).resolves.toBe(expected);
    });

    it("rejects one logical create body above the SQL payload cap", async () => {
      const path = streamPath();
      const response = await putText(
        options,
        path,
        "initial-too-large:".padEnd(options.maxAppendBytes + 1, "i")
      );

      expect(response.status).toBe(413);
    });

    it("rejects a fork prefix above the SQL payload cap", async () => {
      const source = streamPath();
      const child = streamPath();

      await createTextStream(options, source);
      const firstAppend = await appendText(
        options,
        source,
        "x".repeat(options.maxAppendBytes)
      );
      expect(firstAppend.status).toBe(204);
      const tailAppend = await appendText(options, source, "tail");
      expect(tailAppend.status).toBe(204);

      const response = await fetch(`${options.config.baseUrl}${child}`, {
        method: "PUT",
        headers: {
          "Content-Type": "text/plain",
          [STREAM_FORKED_FROM_HEADER]: source,
          [STREAM_FORK_OFFSET_HEADER]: INITIAL_OFFSET,
          [STREAM_FORK_SUB_OFFSET_HEADER]: String(options.maxAppendBytes + 2),
        },
      });

      expect(response.status).toBe(413);
    });

    it("accepts appended data larger than one SQL value", async () => {
      const path = streamPath();
      await createTextStream(options, path);

      let expected = "";
      while (expected.length <= SQL_ROW_LIMIT_BYTES) {
        const chunk = `${expected.length}:`.padEnd(300_000, "x");
        expected += chunk;
        await appendText(options, path, chunk);
      }

      await expect(getText(options, path)).resolves.toBe(expected);
    });

    it("accepts one append larger than one SQL value", async () => {
      const path = streamPath();
      await createTextStream(options, path);
      await appendText(options, path, "ok");

      const spilledAppend = "x".repeat(options.maxChunkBytes + 1);
      const response = await appendText(options, path, spilledAppend);
      expect(response.status).toBe(204);
      await expect(getText(options, path)).resolves.toBe(`ok${spilledAppend}`);
    });

    it("keeps a large JSON append indivisible when reads are bounded", async () => {
      const path = streamPath();
      await createJsonStream(options, path);
      const first = { value: "x".repeat(options.maxReadBytes + 50) };
      const second = { value: "tail" };

      expect(await appendJson(options, path, first)).toBe(204);
      expect(await appendJson(options, path, second)).toBe(204);

      const response = await fetch(`${options.config.baseUrl}${path}`);
      expect(response.status).toBe(200);
      expect(response.headers.get(STREAM_UP_TO_DATE_HEADER)).toBe("false");
      const nextOffset = response.headers.get(STREAM_OFFSET_HEADER);
      expect(nextOffset).toBeTruthy();
      expect(JSON.parse(await response.text())).toEqual([first]);

      const resumed = await fetch(
        `${options.config.baseUrl}${path}?offset=${encodeURIComponent(nextOffset as string)}`
      );
      expect(resumed.status).toBe(200);
      expect(resumed.headers.get(STREAM_UP_TO_DATE_HEADER)).toBe("true");
      expect(JSON.parse(await resumed.text())).toEqual([second]);
    });

    it("rejects a conflicting producer retry after a spilled append", async () => {
      const path = streamPath();
      await createTextStream(options, path);
      const body = "x".repeat(options.maxChunkBytes + 1);
      const producer = {
        [PRODUCER_ID_HEADER]: "producer-a",
        [PRODUCER_EPOCH_HEADER]: "1",
        [PRODUCER_SEQ_HEADER]: "0",
      };

      expect(
        (await appendText(options, path, body, false, producer)).status
      ).toBe(200);
      expect(
        (await appendText(options, path, body, false, producer)).status
      ).toBe(204);
      const conflicting = await appendText(
        options,
        path,
        `${body.slice(0, -1)}y`,
        false,
        producer
      );

      expect(conflicting.status).toBe(409);
      await expect(getText(options, path)).resolves.toBe(body);
    });

    it("keeps closed false until the final read window", async () => {
      const path = streamPath();
      const expected = "closed-window:".padEnd(options.maxReadBytes + 50, "c");

      await createTextStream(options, path);
      const append = await appendText(options, path, expected, true);
      expect(append.status).toBe(204);

      const first = await fetch(`${options.config.baseUrl}${path}`);
      expect(first.status).toBe(200);
      expect(first.headers.get(STREAM_UP_TO_DATE_HEADER)).toBe("false");
      expect(first.headers.get(STREAM_CLOSED_HEADER)).toBeNull();
      const nextOffset = first.headers.get(STREAM_OFFSET_HEADER);
      expect(nextOffset).toBeTruthy();
      await expect(first.text()).resolves.toBe(
        expected.slice(0, options.maxReadBytes)
      );

      const second = await fetch(
        `${options.config.baseUrl}${path}?offset=${encodeURIComponent(nextOffset as string)}`
      );
      expect(second.status).toBe(200);
      expect(second.headers.get(STREAM_UP_TO_DATE_HEADER)).toBe("true");
      expect(second.headers.get(STREAM_CLOSED_HEADER)).toBe("true");
      await expect(second.text()).resolves.toBe(
        expected.slice(options.maxReadBytes)
      );
    });

    it("rejects stale incarnation after delete and recreate", async () => {
      const path = streamPath();
      const created = await createTextStream(options, path, "old");
      const oldIncarnation = created.headers.get(STREAM_INCARNATION_HEADER);
      expect(oldIncarnation?.startsWith("inc_")).toBe(true);

      const deleted = await fetch(`${options.config.baseUrl}${path}`, {
        method: "DELETE",
      });
      expect(deleted.status).toBe(204);

      const recreated = await createTextStream(options, path, "new");
      const newIncarnation = recreated.headers.get(STREAM_INCARNATION_HEADER);
      expect(newIncarnation?.startsWith("inc_")).toBe(true);
      expect(newIncarnation).not.toBe(oldIncarnation);

      const staleRead = await fetch(`${options.config.baseUrl}${path}`, {
        headers: { [STREAM_IF_INCARNATION_HEADER]: oldIncarnation as string },
      });
      expect(staleRead.status).toBe(409);
      const staleLongPoll = await fetch(
        `${options.config.baseUrl}${path}?live=long-poll&offset=${INITIAL_OFFSET}`,
        {
          headers: { [STREAM_IF_INCARNATION_HEADER]: oldIncarnation as string },
        }
      );
      expect(staleLongPoll.status).toBe(409);
      await expect(getText(options, path)).resolves.toBe("new");
    });

    it("releases retained parents when the last fork is deleted", async () => {
      const parent = streamPath();
      const child = streamPath();

      await createTextStream(options, parent, "parent-body");
      const fork = await fetch(`${options.config.baseUrl}${child}`, {
        method: "PUT",
        headers: {
          [STREAM_FORKED_FROM_HEADER]: parent,
          [STREAM_FORK_OFFSET_HEADER]: INITIAL_OFFSET,
          [STREAM_FORK_SUB_OFFSET_HEADER]: "6",
        },
      });
      expect(fork.status).toBe(201);

      const deleteParent = await fetch(`${options.config.baseUrl}${parent}`, {
        method: "DELETE",
      });
      expect(deleteParent.status).toBe(204);

      const deleteChild = await fetch(`${options.config.baseUrl}${child}`, {
        method: "DELETE",
      });
      expect(deleteChild.status).toBe(204);

      const parentHead = await fetch(`${options.config.baseUrl}${parent}`, {
        method: "HEAD",
      });
      expect(parentHead.status).toBe(404);
    });
  });
}

async function createTextStream(
  options: SqlStreamLimitTestOptions,
  path: string,
  body = ""
): Promise<Response> {
  const response = await putText(options, path, body);
  expect(response.status).toBe(201);
  return response;
}

async function putText(
  options: SqlStreamLimitTestOptions,
  path: string,
  body = ""
): Promise<Response> {
  return await fetch(`${options.config.baseUrl}${path}`, {
    method: "PUT",
    headers: { "Content-Type": "text/plain" },
    body,
  });
}

async function createJsonStream(
  options: SqlStreamLimitTestOptions,
  path: string
): Promise<Response> {
  const response = await fetch(`${options.config.baseUrl}${path}`, {
    method: "PUT",
    headers: { "Content-Type": "application/json" },
    body: "[]",
  });
  expect(response.status).toBe(201);
  return response;
}

async function appendJson(
  options: SqlStreamLimitTestOptions,
  path: string,
  body: unknown
): Promise<number> {
  const response = await fetch(`${options.config.baseUrl}${path}`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(body),
  });
  return response.status;
}

async function appendText(
  options: SqlStreamLimitTestOptions,
  path: string,
  body: string,
  close = false,
  headers?: Record<string, string>
): Promise<Response> {
  return await fetch(`${options.config.baseUrl}${path}`, {
    method: "POST",
    headers: {
      "Content-Type": "text/plain",
      ...(close ? { [STREAM_CLOSED_HEADER]: "true" } : {}),
      ...headers,
    },
    body,
  });
}

async function getText(
  options: SqlStreamLimitTestOptions,
  path: string
): Promise<string> {
  let offset: string | null = null;
  let body = "";

  for (let reads = 0; reads < 20; reads += 1) {
    const url =
      offset === null
        ? `${options.config.baseUrl}${path}`
        : `${options.config.baseUrl}${path}?offset=${encodeURIComponent(offset)}`;
    const response = await fetch(url);
    expect(response.status).toBe(200);
    body += await response.text();
    offset = response.headers.get(STREAM_OFFSET_HEADER);
    expect(offset).toBeTruthy();
    if (response.headers.get(STREAM_UP_TO_DATE_HEADER) === "true") {
      return body;
    }
  }

  throw new Error("stream read did not converge");
}

function streamPath(): string {
  return `/storage-layout-${crypto.randomUUID()}`;
}
