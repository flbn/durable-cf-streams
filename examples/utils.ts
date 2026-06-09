import type { AppendResult, Offset, StreamStore } from "durable-cf-streams";
import {
  CACHE_CONTROL_HEADER,
  HEAD_CACHE_CONTROL_VALUE,
  isStreamError,
  isValidOffset,
  normalizeOffset,
  PRODUCER_EPOCH_HEADER,
  PRODUCER_EXPECTED_SEQ_HEADER,
  PRODUCER_RECEIVED_SEQ_HEADER,
  PRODUCER_SEQ_HEADER,
  PROTOCOL_SECURITY_HEADERS,
  ProducerFencedError,
  ProducerSequenceConflictError,
  parseProducerHeaders,
  STREAM_EXPIRES_AT_HEADER,
  STREAM_OFFSET_HEADER,
  STREAM_TTL_HEADER,
  streamErrorStatus,
  TAIL_OFFSET_QUERY_VALUE,
  validateExpiresAt,
  validateTTL,
} from "durable-cf-streams";

export type AsyncQueue = <T>(operation: () => Promise<T>) => Promise<T>;

export const LIVE_WAIT_TIMEOUT_MS = 20_000;

export function createAsyncQueue(): AsyncQueue {
  let tail: Promise<unknown> = Promise.resolve();

  return <T>(operation: () => Promise<T>): Promise<T> => {
    const next = tail.then(operation, operation);
    tail = next.catch(() => undefined);
    return next;
  };
}

export function withProtocolHeaders(response: Response): Response {
  const headers = new Headers(response.headers);
  for (const [name, value] of Object.entries(PROTOCOL_SECURITY_HEADERS)) {
    headers.set(name, value);
  }

  return new Response(response.body, {
    headers,
    status: response.status,
    statusText: response.statusText,
  });
}

export type TtlExpiresResult =
  | { ok: true; ttlSeconds?: number; expiresAt?: string }
  | { ok: false; error: Response };

export function parseTtlAndExpires(request: Request): TtlExpiresResult {
  const ttlHeader = request.headers.get(STREAM_TTL_HEADER);
  const expiresAtHeader = request.headers.get(STREAM_EXPIRES_AT_HEADER);

  if (ttlHeader && expiresAtHeader) {
    return {
      ok: false,
      error: new Response("Cannot specify both TTL and Expires-At", {
        status: 400,
      }),
    };
  }

  let ttlSeconds: number | undefined;
  let expiresAt: string | undefined;

  if (ttlHeader) {
    const parsed = validateTTL(ttlHeader);
    if (parsed === null) {
      return {
        ok: false,
        error: new Response("Invalid TTL value", { status: 400 }),
      };
    }
    ttlSeconds = parsed;
  }

  if (expiresAtHeader) {
    const parsed = validateExpiresAt(expiresAtHeader);
    if (parsed === null) {
      return {
        ok: false,
        error: new Response("Invalid expiresAt value", { status: 400 }),
      };
    }
    expiresAt = parsed.toISOString();
  }

  return { ok: true, ttlSeconds, expiresAt };
}

export type LiveModeResult =
  | { mode: "sse" | "long-poll" | "simple" }
  | { mode: "error"; error: Response };

export function parseLiveMode(
  url: URL,
  request: Request,
  offset: Offset | undefined
): LiveModeResult {
  const liveParam = url.searchParams.get("live");
  const acceptHeader = request.headers.get("accept") ?? "";

  const isSSE =
    liveParam === "sse" || acceptHeader.includes("text/event-stream");
  const isLongPoll = liveParam === "long-poll";

  if ((isSSE || isLongPoll) && offset === undefined) {
    return {
      mode: "error",
      error: new Response("Offset parameter required for live mode", {
        status: 400,
      }),
    };
  }

  if (isSSE) {
    return { mode: "sse" };
  }
  if (isLongPoll) {
    return { mode: "long-poll" };
  }
  return { mode: "simple" };
}

export type OffsetParseResult =
  | { ok: true; offset: Offset | undefined; isTail: boolean }
  | { ok: false; error: Response };

export function parseOffsetParam(
  offsetParam: string | null
): OffsetParseResult {
  if (offsetParam === null) {
    return { ok: true, offset: undefined, isTail: false };
  }
  if (offsetParam === "") {
    return {
      ok: false,
      error: new Response("Empty offset parameter", { status: 400 }),
    };
  }
  if (offsetParam === TAIL_OFFSET_QUERY_VALUE) {
    return { ok: true, offset: undefined, isTail: true };
  }
  if (!isValidOffset(offsetParam)) {
    return {
      ok: false,
      error: new Response("Invalid offset format", { status: 400 }),
    };
  }
  return { ok: true, offset: normalizeOffset(offsetParam), isTail: false };
}

export type ResolvedOffsetResult =
  | { ok: true; offset: Offset | undefined; isTail: boolean }
  | { ok: false; error: Response };

export async function resolveReadOffset(
  store: StreamStore,
  path: string,
  parsed: Extract<OffsetParseResult, { ok: true }>
): Promise<ResolvedOffsetResult> {
  if (!parsed.isTail) {
    return parsed;
  }

  const head = await store.head(path);
  if (!head) {
    return {
      ok: false,
      error: new Response(`Stream not found: ${path}`, { status: 404 }),
    };
  }

  return { ok: true, offset: head.nextOffset, isTail: true };
}

export function parseProducerOptions(request: Request) {
  return parseProducerHeaders(request.headers);
}

export function appendResponse(result: AppendResult): Response {
  const headers = new Headers({
    [STREAM_OFFSET_HEADER]: result.nextOffset,
  });

  if (result.producer) {
    headers.set(PRODUCER_EPOCH_HEADER, String(result.producer.epoch));
    headers.set(PRODUCER_SEQ_HEADER, String(result.producer.seq));
  }

  return new Response(null, {
    status: result.producer && !result.producer.duplicate ? 200 : 204,
    headers,
  });
}

export function tailOffsetCacheHeaders(isTail: boolean): HeadersInit {
  return isTail ? { [CACHE_CONTROL_HEADER]: HEAD_CACHE_CONTROL_VALUE } : {};
}

export function mapError(error: unknown): Response {
  if (isStreamError(error)) {
    const headers = new Headers();

    if (error instanceof ProducerSequenceConflictError) {
      headers.set(PRODUCER_EXPECTED_SEQ_HEADER, error.expected);
      headers.set(PRODUCER_RECEIVED_SEQ_HEADER, error.received);
    }

    if (error instanceof ProducerFencedError) {
      headers.set(PRODUCER_EPOCH_HEADER, String(error.currentEpoch));
    }

    return new Response(error.message, {
      headers,
      status: streamErrorStatus(error),
    });
  }

  if (
    error instanceof Error &&
    (error.message.includes("too large") ||
      error.message.includes("SQLITE_TOOBIG") ||
      error.message.includes("row too big"))
  ) {
    return new Response("Payload too large", { status: 413 });
  }
  console.error("Unexpected error:", error);
  return new Response("Internal Server Error", { status: 500 });
}
