import {
  isValidOffset,
  normalizeOffset,
  STREAM_EXPIRES_AT_HEADER,
  STREAM_TTL_HEADER,
  validateExpiresAt,
  validateTTL,
} from "durable-cf-streams";

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
  offset: string | undefined
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
  | { ok: true; offset: string | undefined }
  | { ok: false; error: Response };

export function parseOffsetParam(
  offsetParam: string | null
): OffsetParseResult {
  if (offsetParam === null) {
    return { ok: true, offset: undefined };
  }
  if (offsetParam === "") {
    return {
      ok: false,
      error: new Response("Empty offset parameter", { status: 400 }),
    };
  }
  if (!isValidOffset(offsetParam)) {
    return {
      ok: false,
      error: new Response("Invalid offset format", { status: 400 }),
    };
  }
  return { ok: true, offset: normalizeOffset(offsetParam) };
}

export function mapError(error: unknown): Response {
  if (error instanceof Error) {
    const tag = (error as { _tag?: string })._tag;
    switch (tag) {
      case "StreamNotFoundError":
        return new Response(error.message, { status: 404 });
      case "SequenceConflictError":
        return new Response(error.message, { status: 409 });
      case "ContentTypeMismatchError":
        return new Response(error.message, { status: 409 });
      case "StreamConflictError":
        return new Response(error.message, { status: 409 });
      case "InvalidJsonError":
        return new Response(error.message, { status: 400 });
      case "PayloadTooLargeError":
        return new Response(error.message, { status: 413 });
      default:
    }

    if (
      error.message.includes("too large") ||
      error.message.includes("SQLITE_TOOBIG") ||
      error.message.includes("row too big")
    ) {
      return new Response("Payload too large", { status: 413 });
    }
  }
  console.error("Unexpected error:", error);
  return new Response("Internal Server Error", { status: 500 });
}
