import type { AppendResult, Offset, StreamStore } from "durable-cf-streams";
import {
  CACHE_CONTROL_HEADER,
  DEFAULT_CONTENT_TYPE,
  encodeSSEData,
  HEAD_CACHE_CONTROL_VALUE,
  isSSETextCompatibleContentType,
  isStreamError,
  isValidOffset,
  normalizeContentType,
  normalizeOffset,
  PRODUCER_EPOCH_HEADER,
  PRODUCER_SEQ_HEADER,
  PROTOCOL_SECURITY_HEADERS,
  parseProducerHeaders,
  RESERVED_CONTROL_PATH_SEGMENT,
  STREAM_CLOSED_HEADER,
  STREAM_EXPIRES_AT_HEADER,
  STREAM_FORK_OFFSET_HEADER,
  STREAM_FORK_SUB_OFFSET_HEADER,
  STREAM_FORKED_FROM_HEADER,
  STREAM_OFFSET_HEADER,
  STREAM_TTL_HEADER,
  streamErrorHeaders,
  streamErrorStatus,
  TAIL_OFFSET_QUERY_VALUE,
  validateExpiresAt,
  validateForkSubOffset,
  validateTTL,
} from "durable-cf-streams";

export type AsyncQueue = <T>(operation: () => Promise<T>) => Promise<T>;

export const LIVE_WAIT_TIMEOUT_MS = 20_000;

export type SSEDataEncoding = "base64";

export function createSSEWriter(
  controller: ReadableStreamDefaultController<Uint8Array>
) {
  const encoder = new TextEncoder();
  let pending = "";
  let scheduled = false;

  const flush = () => {
    scheduled = false;

    if (pending.length === 0) {
      return;
    }

    const chunk = pending;
    pending = "";
    controller.enqueue(encoder.encode(chunk));
  };

  const scheduleFlush = () => {
    if (!scheduled) {
      scheduled = true;
      queueMicrotask(flush);
    }
  };

  return {
    send: (event: string, data: string) => {
      pending += `event: ${event}\n${encodeSSEData(data)}\n\n`;
      scheduleFlush();
    },
    comment: (comment: string) => {
      pending += `: ${comment}\n\n`;
      scheduleFlush();
    },
    flush,
  };
}

const STREAM_ROOT_PATH = "/v1/stream";
const RESERVED_CONTROL_PATH = `${STREAM_ROOT_PATH}/${RESERVED_CONTROL_PATH_SEGMENT}`;

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

export function isReservedControlPath(path: string): boolean {
  return (
    path === RESERVED_CONTROL_PATH ||
    path.startsWith(`${RESERVED_CONTROL_PATH}/`)
  );
}

export function reservedControlResponse(): Response {
  return new Response("Durable Streams control route not found", {
    status: 404,
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

export type ForkOptionsResult =
  | {
      ok: true;
      forkedFrom?: string;
      forkOffset?: Offset;
      forkSubOffset?: number;
    }
  | { ok: false; error: Response };

type ForkSubOffsetResult =
  | { ok: true; forkSubOffset?: number }
  | { ok: false; error: Response };

const hasForkHeaders = (
  forkedFrom: string | null,
  forkOffset: string | null,
  forkSubOffset: string | null
): boolean =>
  forkedFrom !== null || forkOffset !== null || forkSubOffset !== null;

const parseForkSubOffsetHeader = (
  value: string | null,
  forkOffsetHeader: string | null
): ForkSubOffsetResult => {
  if (value === null) {
    return { ok: true };
  }

  const parsed = validateForkSubOffset(value);
  if (parsed === null) {
    return {
      ok: false,
      error: new Response("Invalid fork sub-offset value", { status: 400 }),
    };
  }

  if (parsed > 0 && forkOffsetHeader === null) {
    return {
      ok: false,
      error: new Response("Fork offset required for sub-offset", {
        status: 400,
      }),
    };
  }

  return { ok: true, forkSubOffset: parsed === 0 ? undefined : parsed };
};

export function parseForkOptions(request: Request): ForkOptionsResult {
  const forkedFrom = request.headers.get(STREAM_FORKED_FROM_HEADER);
  const forkOffsetHeader = request.headers.get(STREAM_FORK_OFFSET_HEADER);
  const forkSubOffsetHeader = request.headers.get(
    STREAM_FORK_SUB_OFFSET_HEADER
  );

  if (!hasForkHeaders(forkedFrom, forkOffsetHeader, forkSubOffsetHeader)) {
    return { ok: true };
  }

  if (!forkedFrom) {
    return {
      ok: false,
      error: new Response("Fork source required", { status: 400 }),
    };
  }

  const forkSubOffsetResult = parseForkSubOffsetHeader(
    forkSubOffsetHeader,
    forkOffsetHeader
  );
  if (!forkSubOffsetResult.ok) {
    return forkSubOffsetResult;
  }

  if (forkOffsetHeader === null) {
    return {
      ok: true,
      forkedFrom,
      forkSubOffset: forkSubOffsetResult.forkSubOffset,
    };
  }

  if (!isValidOffset(forkOffsetHeader)) {
    return {
      ok: false,
      error: new Response("Invalid fork offset format", { status: 400 }),
    };
  }

  return {
    ok: true,
    forkedFrom,
    forkOffset: normalizeOffset(forkOffsetHeader),
    forkSubOffset: forkSubOffsetResult.forkSubOffset,
  };
}

export function parsePutContentType(
  request: Request,
  forkedFrom: string | undefined
): string | undefined {
  const contentType = request.headers.get("content-type");
  if (contentType) {
    return normalizeContentType(contentType);
  }

  return forkedFrom === undefined ? DEFAULT_CONTENT_TYPE : undefined;
}

export type LiveModeResult =
  | { mode: "sse" }
  | { mode: "long-poll" | "simple" }
  | { mode: "error"; error: Response };

function requestedLiveMode(
  liveParam: string | null,
  acceptHeader: string
): "sse" | "long-poll" | "simple" {
  if (liveParam === "sse" || acceptHeader.includes("text/event-stream")) {
    return "sse";
  }
  return liveParam === "long-poll" ? "long-poll" : "simple";
}

export function parseLiveMode(
  url: URL,
  request: Request,
  offset: Offset | undefined
): LiveModeResult {
  const mode = requestedLiveMode(
    url.searchParams.get("live"),
    request.headers.get("accept") ?? ""
  );

  if (mode !== "simple" && offset === undefined) {
    return {
      mode: "error",
      error: new Response("Offset parameter required for live mode", {
        status: 400,
      }),
    };
  }

  return { mode };
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

export type SSEEncodingResult =
  | { ok: true; encoding: SSEDataEncoding | undefined }
  | { ok: false; error: Response };

export async function resolveSSEEncoding(
  store: StreamStore,
  path: string
): Promise<SSEEncodingResult> {
  const head = await store.head(path);
  if (!head) {
    return {
      ok: false,
      error: new Response(`Stream not found: ${path}`, { status: 404 }),
    };
  }

  return {
    ok: true,
    encoding: isSSETextCompatibleContentType(head.contentType)
      ? undefined
      : "base64",
  };
}

type ResolvedLiveMode =
  | { mode: "sse"; encoding: SSEDataEncoding | undefined }
  | { mode: "long-poll" | "simple" };

export type ReadRequestResult =
  | {
      ok: true;
      offset: Offset | undefined;
      isTail: boolean;
      liveMode: ResolvedLiveMode;
    }
  | { ok: false; error: Response };

export async function resolveReadRequest(
  store: StreamStore,
  path: string,
  url: URL,
  request: Request
): Promise<ReadRequestResult> {
  const offsetResult = parseOffsetParam(url.searchParams.get("offset"));
  if (!offsetResult.ok) {
    return offsetResult;
  }

  const resolvedOffset = await resolveReadOffset(store, path, offsetResult);
  if (!resolvedOffset.ok) {
    return resolvedOffset;
  }

  const liveMode = parseLiveMode(url, request, resolvedOffset.offset);
  if (liveMode.mode === "error") {
    return { ok: false, error: liveMode.error };
  }

  if (liveMode.mode !== "sse") {
    return {
      ok: true,
      offset: resolvedOffset.offset,
      isTail: resolvedOffset.isTail,
      liveMode,
    };
  }

  const encodingResult = await resolveSSEEncoding(store, path);
  if (!encodingResult.ok) {
    return encodingResult;
  }

  return {
    ok: true,
    offset: resolvedOffset.offset,
    isTail: resolvedOffset.isTail,
    liveMode: { mode: "sse", encoding: encodingResult.encoding },
  };
}

type SSELoopState = {
  currentOffset: Offset;
};

export type SendSSEControl = (offset: Offset, closed?: boolean) => void;
export type SendSSEData = (data: Uint8Array, contentType: string) => void;

export async function sendSSESnapshot(
  store: StreamStore,
  path: string,
  state: SSELoopState,
  sendControl: SendSSEControl,
  sendData: SendSSEData
): Promise<boolean> {
  const result = await store.get(path, { offset: state.currentOffset });

  if (result.messages.length > 0) {
    const body = store.formatResponse(path, result.messages);
    sendData(body, result.contentType);
    state.currentOffset = result.nextOffset;
  }

  sendControl(state.currentOffset, result.closed);
  return result.closed;
}

async function sendSSETimeoutControl(
  store: StreamStore,
  path: string,
  state: SSELoopState,
  sendControl: SendSSEControl
): Promise<boolean> {
  const current = await store.get(path, { offset: state.currentOffset });
  sendControl(current.nextOffset, current.closed);
  return current.closed;
}

export async function pumpSSEStream(
  store: StreamStore,
  path: string,
  state: SSELoopState & { cancelled: boolean },
  timeoutMs: number,
  sendControl: SendSSEControl,
  sendData: SendSSEData
): Promise<void> {
  if (!store.has(path)) {
    throw new Error("Stream not found");
  }

  if (await sendSSESnapshot(store, path, state, sendControl, sendData)) {
    return;
  }

  while (!state.cancelled) {
    if (!store.has(path)) {
      throw new Error("Stream not found");
    }

    const wait = await store.waitForData(path, state.currentOffset, timeoutMs);
    const closed = wait.timedOut
      ? await sendSSETimeoutControl(store, path, state, sendControl)
      : await sendSSESnapshot(store, path, state, sendControl, sendData);

    if (closed) {
      return;
    }
  }
}

export function parseProducerOptions(request: Request) {
  return parseProducerHeaders(request.headers);
}

export function isStreamClosedRequest(request: Request): boolean {
  return request.headers.get(STREAM_CLOSED_HEADER)?.toLowerCase() === "true";
}

export function streamClosedHeaders(closed: boolean | undefined): HeadersInit {
  return closed === true ? { [STREAM_CLOSED_HEADER]: "true" } : {};
}

export function streamMetadataHeaders(result: {
  readonly closed?: boolean;
  readonly ttlSeconds?: number;
  readonly expiresAt?: string;
}): HeadersInit {
  return {
    ...streamClosedHeaders(result.closed),
    ...(result.ttlSeconds === undefined
      ? {}
      : { [STREAM_TTL_HEADER]: String(result.ttlSeconds) }),
    ...(result.expiresAt === undefined
      ? {}
      : { [STREAM_EXPIRES_AT_HEADER]: result.expiresAt }),
  };
}

export function appendResponse(result: AppendResult): Response {
  const headers = new Headers({
    [STREAM_OFFSET_HEADER]: result.nextOffset,
  });

  if (result.closed === true) {
    headers.set(STREAM_CLOSED_HEADER, "true");
  }

  if (result.producer) {
    headers.set(PRODUCER_EPOCH_HEADER, String(result.producer.epoch));
    headers.set(PRODUCER_SEQ_HEADER, String(result.producer.seq));
  }

  return new Response(null, {
    status:
      result.producer && !result.producer.duplicate && result.appended === true
        ? 200
        : 204,
    headers,
  });
}

export function tailOffsetCacheHeaders(isTail: boolean): HeadersInit {
  return isTail ? { [CACHE_CONTROL_HEADER]: HEAD_CACHE_CONTROL_VALUE } : {};
}

export function mapError(error: unknown): Response {
  if (isStreamError(error)) {
    return new Response(error.message, {
      headers: streamErrorHeaders(error),
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
