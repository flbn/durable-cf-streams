import { Match } from "effect";
import {
  PRODUCER_EPOCH_HEADER,
  PRODUCER_EXPECTED_SEQ_HEADER,
  PRODUCER_RECEIVED_SEQ_HEADER,
  STREAM_CLOSED_HEADER,
  STREAM_OFFSET_HEADER,
} from "./const.js";

export class StreamNotFoundError extends Error {
  readonly _tag = "StreamNotFoundError" as const;
  readonly path: string;

  constructor(path: string) {
    super(`Stream not found: ${path}`);
    this.name = "StreamNotFoundError";
    this.path = path;
  }
}

export class StreamConflictError extends Error {
  readonly _tag = "StreamConflictError" as const;

  constructor(message: string) {
    super(message);
    this.name = "StreamConflictError";
  }
}

export class StreamClosedError extends Error {
  readonly _tag = "StreamClosedError" as const;
  readonly path: string;
  readonly nextOffset: string;

  constructor(path: string, nextOffset: string) {
    super(`Stream closed: ${path}`);
    this.name = "StreamClosedError";
    this.path = path;
    this.nextOffset = nextOffset;
  }
}

export class StreamGoneError extends Error {
  readonly _tag = "StreamGoneError" as const;
  readonly path: string;

  constructor(path: string) {
    super(`Stream gone: ${path}`);
    this.name = "StreamGoneError";
    this.path = path;
  }
}

export class SequenceConflictError extends Error {
  readonly _tag = "SequenceConflictError" as const;
  readonly expected: string;
  readonly received: string;

  constructor(expected: string, received: string) {
    super(`Sequence conflict: expected ${expected}, received ${received}`);
    this.name = "SequenceConflictError";
    this.expected = expected;
    this.received = received;
  }
}

export class ContentTypeMismatchError extends Error {
  readonly _tag = "ContentTypeMismatchError" as const;
  readonly expected: string;
  readonly received: string;

  constructor(expected: string, received: string) {
    super(`Content-Type mismatch: expected ${expected}, received ${received}`);
    this.name = "ContentTypeMismatchError";
    this.expected = expected;
    this.received = received;
  }
}

export class InvalidJsonError extends Error {
  readonly _tag = "InvalidJsonError" as const;

  constructor(message: string) {
    super(message);
    this.name = "InvalidJsonError";
  }
}

export class InvalidOffsetError extends Error {
  readonly _tag = "InvalidOffsetError" as const;

  constructor(offset: string) {
    super(`Invalid offset format: ${offset}`);
    this.name = "InvalidOffsetError";
  }
}

export class InvalidProducerError extends Error {
  readonly _tag = "InvalidProducerError" as const;

  constructor(message: string) {
    super(message);
    this.name = "InvalidProducerError";
  }
}

export class ProducerSequenceConflictError extends Error {
  readonly _tag = "ProducerSequenceConflictError" as const;
  readonly expected: string;
  readonly received: string;

  constructor(expected: string, received: string) {
    super(
      `Producer sequence conflict: expected ${expected}, received ${received}`
    );
    this.name = "ProducerSequenceConflictError";
    this.expected = expected;
    this.received = received;
  }
}

export class ProducerFencedError extends Error {
  readonly _tag = "ProducerFencedError" as const;
  readonly currentEpoch: number;
  readonly receivedEpoch: number;

  constructor(currentEpoch: number, receivedEpoch: number) {
    super(
      `Producer fenced: current epoch ${currentEpoch}, received ${receivedEpoch}`
    );
    this.name = "ProducerFencedError";
    this.currentEpoch = currentEpoch;
    this.receivedEpoch = receivedEpoch;
  }
}

export class PayloadTooLargeError extends Error {
  readonly _tag = "PayloadTooLargeError" as const;
  readonly maxBytes: number;
  readonly receivedBytes: number;

  constructor(maxBytes: number, receivedBytes: number) {
    super(
      `Payload too large: max ${maxBytes} bytes, received ${receivedBytes} bytes`
    );
    this.name = "PayloadTooLargeError";
    this.maxBytes = maxBytes;
    this.receivedBytes = receivedBytes;
  }
}

export type StreamError =
  | StreamNotFoundError
  | StreamClosedError
  | StreamGoneError
  | StreamConflictError
  | SequenceConflictError
  | ContentTypeMismatchError
  | InvalidJsonError
  | InvalidOffsetError
  | InvalidProducerError
  | ProducerSequenceConflictError
  | ProducerFencedError
  | PayloadTooLargeError;

const streamErrorTags = new Set<StreamError["_tag"]>([
  "StreamNotFoundError",
  "StreamClosedError",
  "StreamGoneError",
  "StreamConflictError",
  "SequenceConflictError",
  "ContentTypeMismatchError",
  "InvalidJsonError",
  "InvalidOffsetError",
  "InvalidProducerError",
  "ProducerSequenceConflictError",
  "ProducerFencedError",
  "PayloadTooLargeError",
]);

export const isStreamError = (error: unknown): error is StreamError =>
  error instanceof Error &&
  typeof (error as { _tag?: unknown })._tag === "string" &&
  streamErrorTags.has(
    (error as { _tag?: unknown })._tag as StreamError["_tag"]
  );

export const streamErrorStatus = Match.type<StreamError>().pipe(
  Match.tag("StreamNotFoundError", () => 404),
  Match.tag("StreamGoneError", () => 410),
  Match.tag(
    "StreamClosedError",
    "StreamConflictError",
    "SequenceConflictError",
    "ContentTypeMismatchError",
    "ProducerSequenceConflictError",
    () => 409
  ),
  Match.tag(
    "InvalidJsonError",
    "InvalidOffsetError",
    "InvalidProducerError",
    () => 400
  ),
  Match.tag("ProducerFencedError", () => 403),
  Match.tag("PayloadTooLargeError", () => 413),
  Match.exhaustive
);

export const streamErrorHeaders: (
  error: StreamError
) => Record<string, string> = Match.type<StreamError>().pipe(
  Match.tag("StreamClosedError", (error) => ({
    [STREAM_CLOSED_HEADER]: "true",
    [STREAM_OFFSET_HEADER]: error.nextOffset,
  })),
  Match.tag("ProducerSequenceConflictError", (error) => ({
    [PRODUCER_EXPECTED_SEQ_HEADER]: error.expected,
    [PRODUCER_RECEIVED_SEQ_HEADER]: error.received,
  })),
  Match.tag("ProducerFencedError", (error) => ({
    [PRODUCER_EPOCH_HEADER]: String(error.currentEpoch),
  })),
  Match.tag(
    "StreamNotFoundError",
    "StreamGoneError",
    "StreamConflictError",
    "SequenceConflictError",
    "ContentTypeMismatchError",
    "InvalidJsonError",
    "InvalidOffsetError",
    "InvalidProducerError",
    "PayloadTooLargeError",
    () => ({})
  ),
  Match.exhaustive
);
