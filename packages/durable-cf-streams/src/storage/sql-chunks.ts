import { StreamConflictError } from "../errors.js";
import {
  formatOffset,
  initialOffset,
  offsetToBytePos,
  parseOffset,
} from "../offsets.js";
import type { Offset, StreamIncarnation, StreamMessage } from "../types.js";

export type SqlChunkRow = {
  readonly append_index: number;
  readonly incarnation: string;
  readonly chunk_index: number;
  readonly chunk_count: number;
  readonly start_pos: number;
  readonly end_pos: number;
  readonly start_offset: Offset;
  readonly end_offset: Offset;
  readonly data: ArrayBuffer;
};

type SqlChunkReadGroup = {
  appendIndex: number;
  chunkCount: number;
  nextChunkIndex: number;
  sawFinalChunk: boolean;
};

export type SqlChunkReadContinuity = {
  group: SqlChunkReadGroup | undefined;
};

export type SqlChunkMessagesResult = {
  readonly messages: StreamMessage[];
  readonly nextOffset: Offset;
};

type SqlChunkMessage = {
  readonly appendIndex: number;
  readonly message: StreamMessage;
  readonly nextOffset: Offset;
  readonly endPos: number;
  readonly startPos: number;
};

type PendingMessage = {
  readonly appendIndex: number;
  readonly offset: Offset;
  readonly parts: Uint8Array[];
  readonly byteLength: number;
};

export const createSqlChunkReadContinuity = (): SqlChunkReadContinuity => ({
  group: undefined,
});

/**
 * rebuilds stream messages from SQL chunk rows.
 * NOTE: this is the corruption boundary for SQL stores; missing, overlapping, malformed, or out-of-order chunks fail before callers receive bytes.
 */
export const readSqlChunkMessages = (
  path: string,
  startOffset: Offset,
  incarnation: StreamIncarnation,
  chunks: readonly SqlChunkRow[],
  endPos?: number,
  requireCompleteGroups = endPos === undefined
): SqlChunkMessagesResult => {
  const startPos = offsetToBytePos(startOffset);
  const limitEndPos = endPos ?? Number.POSITIVE_INFINITY;
  const messages: StreamMessage[] = [];
  let expectedStart = startPos;
  let nextOffset = startOffset;
  let pending: PendingMessage | undefined;
  const chunkContinuity = createSqlChunkReadContinuity();

  for (const chunk of chunks) {
    if (chunk.incarnation !== incarnation) {
      throw new StreamConflictError(
        `stream chunk incarnation is stale: ${path}`
      );
    }
    assertSqlChunkReadContinuity(path, chunkContinuity, chunk, {
      requireCompleteGroups,
    });
    const result = chunkMessage(startOffset, startPos, limitEndPos, chunk);
    if (result === undefined) {
      continue;
    }
    if (result.startPos !== expectedStart) {
      throw new StreamConflictError(
        `stream chunk range is incomplete: ${path}`
      );
    }

    pending = appendPendingMessage(messages, pending, result);
    nextOffset = result.nextOffset;
    expectedStart = result.endPos;
    if (expectedStart >= limitEndPos) {
      break;
    }
  }

  assertSqlChunkReadFinished(path, chunkContinuity, {
    requireCompleteGroups,
  });
  if (endPos !== undefined && expectedStart < limitEndPos) {
    throw new StreamConflictError(`stream chunk range is incomplete: ${path}`);
  }
  flushPendingMessage(messages, pending);

  return { messages, nextOffset };
};

/**
 * checks SQL chunk rows before bytes leave storage.
 * NOTE: bounded reads may begin or end inside one logical append, so only completed groups must match their recorded count.
 */
export const assertSqlChunkReadContinuity = (
  path: string,
  state: SqlChunkReadContinuity,
  chunk: SqlChunkRow,
  options: { readonly requireCompleteGroups: boolean }
): void => {
  assertSqlChunkMetadata(path, chunk);

  if (
    state.group !== undefined &&
    state.group.appendIndex !== chunk.append_index
  ) {
    assertSqlChunkReadGroupComplete(path, state.group, options);
    state.group = undefined;
  }

  if (state.group === undefined) {
    if (options.requireCompleteGroups && chunk.chunk_index !== 0) {
      throw new StreamConflictError(
        `stream chunk rows are not contiguous: ${path}`
      );
    }
    state.group = {
      appendIndex: chunk.append_index,
      chunkCount: chunk.chunk_count,
      nextChunkIndex: chunk.chunk_index,
      sawFinalChunk: false,
    };
  }

  if (state.group.chunkCount !== chunk.chunk_count) {
    throw new StreamConflictError(
      `stream chunk rows do not match the recorded count: ${path}`
    );
  }
  if (state.group.nextChunkIndex !== chunk.chunk_index) {
    throw new StreamConflictError(
      `stream chunk rows are not contiguous: ${path}`
    );
  }

  state.group.nextChunkIndex = chunk.chunk_index + 1;
  state.group.sawFinalChunk = chunk.chunk_index === chunk.chunk_count - 1;
};

export const assertSqlChunkReadFinished = (
  path: string,
  state: SqlChunkReadContinuity,
  options: { readonly requireCompleteGroups: boolean }
): void => {
  if (state.group === undefined) {
    return;
  }
  assertSqlChunkReadGroupComplete(path, state.group, options);
};

const assertSqlChunkReadGroupComplete = (
  path: string,
  group: SqlChunkReadGroup,
  options: { readonly requireCompleteGroups: boolean }
): void => {
  if (
    (options.requireCompleteGroups || group.sawFinalChunk) &&
    group.nextChunkIndex !== group.chunkCount
  ) {
    throw new StreamConflictError(
      `stream chunk rows do not match the recorded count: ${path}`
    );
  }
};

const assertSqlChunkMetadata = (path: string, chunk: SqlChunkRow): void => {
  if (
    !Number.isSafeInteger(chunk.append_index) ||
    chunk.append_index <= 0 ||
    typeof chunk.incarnation !== "string" ||
    chunk.incarnation.length === 0 ||
    !Number.isSafeInteger(chunk.chunk_index) ||
    chunk.chunk_index < 0 ||
    !Number.isSafeInteger(chunk.chunk_count) ||
    chunk.chunk_count <= 0 ||
    chunk.chunk_index >= chunk.chunk_count ||
    !Number.isSafeInteger(chunk.start_pos) ||
    chunk.start_pos < 0 ||
    !Number.isSafeInteger(chunk.end_pos) ||
    chunk.end_pos <= chunk.start_pos ||
    new Uint8Array(chunk.data).length !== chunk.end_pos - chunk.start_pos ||
    offsetToBytePos(chunk.start_offset) !== chunk.start_pos ||
    offsetToBytePos(chunk.end_offset) !== chunk.end_pos ||
    !hasExpectedChunkOffsetSeq(chunk)
  ) {
    throw new StreamConflictError(
      `stream chunk metadata is malformed: ${path}`
    );
  }
};

const hasExpectedChunkOffsetSeq = (chunk: SqlChunkRow): boolean => {
  const startOffset = parseOffset(chunk.start_offset);
  const endOffset = parseOffset(chunk.end_offset);
  if (startOffset === null || endOffset === null) {
    return false;
  }

  if (endOffset.seq !== chunk.append_index) {
    return false;
  }
  if (chunk.start_pos === 0) {
    return chunk.chunk_index === 0 && chunk.start_offset === initialOffset();
  }
  const expectedStartSeq =
    chunk.chunk_index === 0 ? chunk.append_index - 1 : chunk.append_index;
  if (startOffset.seq !== expectedStartSeq) {
    return false;
  }
  return chunk.start_offset !== initialOffset();
};

const chunkMessageRange = (
  streamStartPos: number,
  limitEndPos: number,
  chunk: SqlChunkRow
): { readonly startPos: number; readonly endPos: number } | undefined => {
  const startPos = Math.max(streamStartPos, chunk.start_pos);
  const endPos = Math.min(limitEndPos, chunk.end_pos);
  return startPos < endPos ? { startPos, endPos } : undefined;
};

const chunkMessageOffset = (
  startOffset: Offset,
  streamStartPos: number,
  chunk: SqlChunkRow,
  messageStartPos: number
): Offset => {
  if (messageStartPos === streamStartPos) {
    return startOffset;
  }
  return messageStartPos === chunk.start_pos
    ? chunk.start_offset
    : formatOffset(0, messageStartPos);
};

const chunkMessageNextOffset = (
  chunk: SqlChunkRow,
  messageEndPos: number
): Offset =>
  messageEndPos === chunk.end_pos
    ? chunk.end_offset
    : formatOffset(0, messageEndPos);

const chunkMessage = (
  startOffset: Offset,
  streamStartPos: number,
  limitEndPos: number,
  chunk: SqlChunkRow
): SqlChunkMessage | undefined => {
  const range = chunkMessageRange(streamStartPos, limitEndPos, chunk);
  if (range === undefined) {
    return;
  }

  const chunkData = new Uint8Array(chunk.data);
  return {
    appendIndex: chunk.append_index,
    startPos: range.startPos,
    endPos: range.endPos,
    nextOffset: chunkMessageNextOffset(chunk, range.endPos),
    message: {
      offset: chunkMessageOffset(
        startOffset,
        streamStartPos,
        chunk,
        range.startPos
      ),
      timestamp: Date.now(),
      data: chunkData.slice(
        range.startPos - chunk.start_pos,
        range.endPos - chunk.start_pos
      ),
    },
  };
};

const appendPendingMessage = (
  messages: StreamMessage[],
  pending: PendingMessage | undefined,
  chunk: SqlChunkMessage
): PendingMessage => {
  if (pending === undefined || pending.appendIndex !== chunk.appendIndex) {
    flushPendingMessage(messages, pending);
    return {
      appendIndex: chunk.appendIndex,
      offset: chunk.message.offset,
      parts: [chunk.message.data],
      byteLength: chunk.message.data.length,
    };
  }

  pending.parts.push(chunk.message.data);
  return {
    ...pending,
    byteLength: pending.byteLength + chunk.message.data.length,
  };
};

const flushPendingMessage = (
  messages: StreamMessage[],
  pending: PendingMessage | undefined
): void => {
  if (pending === undefined) {
    return;
  }

  const data = new Uint8Array(pending.byteLength);
  let offset = 0;
  for (const part of pending.parts) {
    data.set(part, offset);
    offset += part.length;
  }
  messages.push({ offset: pending.offset, timestamp: Date.now(), data });
};
