import { DEFAULT_CONTENT_TYPE } from "../const.js";
import {
  ContentTypeMismatchError,
  InvalidOffsetError,
  PayloadTooLargeError,
  SequenceConflictError,
  StreamClosedError,
  StreamConflictError,
  StreamGoneError,
} from "../errors.js";
import { formatOffset, initialOffset, parseOffset } from "../offsets.js";
import type { ProducerAppendDecision } from "../producer.js";
import {
  isJsonContentType,
  normalizeContentType,
  processJsonAppend,
  validateJsonCreate,
} from "../protocol.js";
import { OffsetSchema, StreamIncarnationSchema } from "../schema.js";
import type {
  AppendOptions,
  AppendResult,
  Offset,
  ProducerAppendOptions,
  PutOptions,
  StreamIncarnation,
  StreamMessage,
} from "../types.js";

export type IdempotentCreateInfo = {
  readonly contentType: string;
  readonly ttlSeconds?: number;
  readonly expiresAt?: string;
  readonly closed?: boolean;
  readonly forkedFrom?: string;
  readonly forkOffset?: Offset;
  readonly forkSubOffset?: number;
  readonly deleted?: boolean;
};

export type ExpirationMetadata = {
  readonly ttlSeconds?: number;
  readonly expiresAt?: string;
};

export const assertStreamLive = (
  path: string,
  info: { readonly deleted?: boolean }
): void => {
  if (info.deleted === true) {
    throw new StreamGoneError(path);
  }
};

export const generateIncarnation = (): StreamIncarnation => {
  const bytes = new Uint8Array(16);
  crypto.getRandomValues(bytes);
  return StreamIncarnationSchema.make(
    `inc_${[...bytes].map((byte) => byte.toString(16).padStart(2, "0")).join("")}`
  );
};

export const assertStreamIncarnation = (
  path: string,
  actual: StreamIncarnation,
  expected: StreamIncarnation | undefined
): void => {
  if (expected !== undefined && actual !== expected) {
    throw new StreamConflictError(`stream incarnation is stale: ${path}`);
  }
};

export const validateReadOffset = (
  offset: Offset,
  tailOffset: Offset
): { readonly byteOffset: number; readonly tailPos: number } => {
  const parsedOffset = parseOffset(offset);
  const parsedTailOffset = parseOffset(tailOffset);
  if (
    !(
      parsedOffset &&
      parsedTailOffset &&
      Number.isSafeInteger(parsedOffset.seq) &&
      Number.isSafeInteger(parsedOffset.pos) &&
      Number.isSafeInteger(parsedTailOffset.seq) &&
      Number.isSafeInteger(parsedTailOffset.pos)
    )
  ) {
    throw new InvalidOffsetError(offset);
  }
  if (
    parsedOffset.seq > parsedTailOffset.seq ||
    parsedOffset.pos > parsedTailOffset.pos
  ) {
    throw new InvalidOffsetError(offset);
  }
  return { byteOffset: parsedOffset.pos, tailPos: parsedTailOffset.pos };
};

export const inheritedExpiration = (
  source: ExpirationMetadata,
  options: PutOptions
): ExpirationMetadata => {
  if (options.ttlSeconds !== undefined) {
    return { ttlSeconds: options.ttlSeconds };
  }

  if (options.expiresAt !== undefined) {
    return { expiresAt: options.expiresAt };
  }

  return source;
};

export const validateIdempotentCreate = (
  existing: IdempotentCreateInfo,
  options: PutOptions
): void => {
  const existingNormalized = normalizeContentType(existing.contentType);
  const requestedContentType =
    options.contentType ??
    (options.forkedFrom === undefined
      ? DEFAULT_CONTENT_TYPE
      : existing.contentType);
  const reqNormalized = normalizeContentType(requestedContentType);

  if (existingNormalized !== reqNormalized) {
    throw new ContentTypeMismatchError(existingNormalized, reqNormalized);
  }

  if (options.forkedFrom !== undefined) {
    validateIdempotentForkCreate(existing, options);
    return;
  }

  validateIdempotentRegularCreate(existing, options);

  if ((options.closed ?? false) !== (existing.closed ?? false)) {
    throw new StreamConflictError("closed state mismatch on idempotent create");
  }
};

const validateIdempotentRegularCreate = (
  existing: IdempotentCreateInfo,
  options: PutOptions
): void => {
  if (existing.forkedFrom !== undefined) {
    throw new StreamConflictError("fork source mismatch on idempotent create");
  }

  if (options.ttlSeconds !== existing.ttlSeconds) {
    throw new StreamConflictError("TTL mismatch on idempotent create");
  }

  if (options.expiresAt !== existing.expiresAt) {
    throw new StreamConflictError("Expires-At mismatch on idempotent create");
  }
};

const validateIdempotentForkCreate = (
  existing: IdempotentCreateInfo,
  options: PutOptions
): void => {
  if (options.forkedFrom !== existing.forkedFrom) {
    throw new StreamConflictError("fork source mismatch on idempotent create");
  }

  if (
    options.forkOffset !== undefined &&
    options.forkOffset !== existing.forkOffset
  ) {
    throw new StreamConflictError("fork offset mismatch on idempotent create");
  }

  if (
    normalizeForkSubOffset(options.forkSubOffset) !==
    normalizeForkSubOffset(existing.forkSubOffset)
  ) {
    throw new StreamConflictError(
      "fork sub-offset mismatch on idempotent create"
    );
  }

  if (
    options.ttlSeconds !== undefined &&
    options.ttlSeconds !== existing.ttlSeconds
  ) {
    throw new StreamConflictError("TTL mismatch on idempotent create");
  }

  if (
    options.expiresAt !== undefined &&
    options.expiresAt !== existing.expiresAt
  ) {
    throw new StreamConflictError("Expires-At mismatch on idempotent create");
  }
};

export type PreparedData = {
  readonly data: Uint8Array;
  readonly appendCount: number;
  readonly nextOffset: Offset;
};

export type WholeValueCreateSource = ExpirationMetadata & {
  readonly contentType: string;
  readonly data: Uint8Array;
  readonly nextOffset: Offset;
  readonly appendEndPositions?: readonly number[];
};

export type PreparedWholeValueCreate = ExpirationMetadata & {
  readonly contentType: string;
  readonly closed: boolean;
  readonly forkOffset?: Offset;
  readonly forkSubOffset?: number;
  readonly data: Uint8Array;
  readonly appendCount: number;
  readonly nextOffset: Offset;
  readonly appendEndPositions: number[];
};

export const normalizeForkSubOffset = (
  forkSubOffset: number | undefined
): number | undefined =>
  forkSubOffset === undefined || forkSubOffset === 0
    ? undefined
    : forkSubOffset;

const concatenateData = (left: Uint8Array, right: Uint8Array): Uint8Array => {
  const result = new Uint8Array(left.length + right.length);
  result.set(left);
  result.set(right, left.length);
  return result;
};

const RECEIPT_OFFSET_LENGTH = initialOffset().length;
const RECEIPT_HEADER_LENGTH = 1 + RECEIPT_OFFSET_LENGTH;

export type ProducerAppendReceipt = {
  readonly endOffset: Offset;
  readonly data: Uint8Array;
  readonly closed: boolean;
};

export const bytesEqual = (left: Uint8Array, right: Uint8Array): boolean => {
  if (left.length !== right.length) {
    return false;
  }
  for (let index = 0; index < left.length; index += 1) {
    if (left[index] !== right[index]) {
      return false;
    }
  }
  return true;
};

export const assertPayloadSize = (
  maxBytes: number,
  receivedBytes: number
): void => {
  if (receivedBytes > maxBytes) {
    throw new PayloadTooLargeError(maxBytes, receivedBytes);
  }
};

export const producerAppendReceiptKey = (
  producer: ProducerAppendOptions
): string => `${producer.id}\u0000${producer.epoch}\u0000${producer.seq}`;

export const prepareAppendPayload = (
  data: Uint8Array,
  contentType: string
): Uint8Array => {
  if (data.length === 0) {
    return data;
  }
  return isJsonContentType(contentType)
    ? processJsonAppend(new Uint8Array(0), data)
    : data;
};

export const assertProducerAppendReceiptMatches = (
  path: string,
  stored: ProducerAppendReceipt | undefined,
  actualData: Uint8Array,
  closed: boolean
): ProducerAppendReceipt => {
  if (stored === undefined) {
    throw new StreamConflictError(`producer retry receipt is missing: ${path}`);
  }
  if (stored.closed !== closed || !bytesEqual(stored.data, actualData)) {
    throw new StreamConflictError(
      `producer retry does not match original append: ${path}`
    );
  }
  return stored;
};

export const encodeProducerAppendReceipt = (
  receipt: ProducerAppendReceipt
): Uint8Array => {
  const offset = new TextEncoder().encode(receipt.endOffset);
  if (offset.length !== RECEIPT_OFFSET_LENGTH) {
    throw new StreamConflictError("producer retry receipt offset is malformed");
  }
  const encoded = new Uint8Array(RECEIPT_HEADER_LENGTH + receipt.data.length);
  encoded[0] = receipt.closed ? 1 : 0;
  encoded.set(offset, 1);
  encoded.set(receipt.data, RECEIPT_HEADER_LENGTH);
  return encoded;
};

export const decodeProducerAppendReceipt = (
  path: string,
  data: Uint8Array | null
): ProducerAppendReceipt | undefined => {
  if (data === null) {
    return;
  }
  if (data.length < RECEIPT_HEADER_LENGTH) {
    throw new StreamConflictError(
      `producer retry receipt is malformed: ${path}`
    );
  }
  const closedByte = data[0];
  if (closedByte !== 0 && closedByte !== 1) {
    throw new StreamConflictError(
      `producer retry receipt is malformed: ${path}`
    );
  }
  const endOffset = OffsetSchema.make(
    new TextDecoder().decode(data.slice(1, RECEIPT_HEADER_LENGTH))
  );
  return {
    endOffset,
    closed: closedByte === 1,
    data: data.slice(RECEIPT_HEADER_LENGTH),
  };
};

export const readWholeValueWindow = (
  data: Uint8Array,
  offset: Offset,
  tailOffset: Offset,
  closed: boolean | undefined,
  maxReadBytes: number,
  contentType?: string,
  appendEndPositions: readonly number[] = []
): {
  readonly messages: StreamMessage[];
  readonly nextOffset: Offset;
  readonly upToDate: boolean;
  readonly closed: boolean | undefined;
} => {
  const { byteOffset, tailPos } = validateReadOffset(offset, tailOffset);
  const visibleLength = Math.min(data.length, tailPos);
  if (byteOffset >= visibleLength) {
    return {
      messages: [],
      nextOffset: tailOffset,
      upToDate: true,
      closed,
    };
  }

  let end = Math.min(byteOffset + maxReadBytes, visibleLength);
  if (contentType !== undefined && isJsonContentType(contentType)) {
    end = jsonWindowEnd(byteOffset, end, visibleLength, appendEndPositions);
  }
  const upToDate = end === visibleLength;
  return {
    messages: [
      {
        offset,
        timestamp: Date.now(),
        data: data.slice(byteOffset, end),
      },
    ],
    nextOffset: upToDate
      ? tailOffset
      : formatOffset(appendSequenceForPosition(end, appendEndPositions), end),
    upToDate,
    closed: upToDate ? closed : false,
  };
};

const jsonWindowEnd = (
  start: number,
  proposedEnd: number,
  tail: number,
  appendEndPositions: readonly number[]
): number => {
  if (proposedEnd >= tail) {
    return tail;
  }
  const boundary = appendEndPositions.find(
    (position) => position >= proposedEnd && position > start
  );
  return boundary ?? tail;
};

const appendSequenceForPosition = (
  end: number,
  appendEndPositions: readonly number[]
): number => {
  const index = appendEndPositions.findIndex((position) => position >= end);
  return index === -1 ? 0 : index + 1;
};

export const resolveCreateContentType = (options: PutOptions): string =>
  options.contentType ?? DEFAULT_CONTENT_TYPE;

export const prepareInitialData = (options: PutOptions): PreparedData => {
  let data = options.data ?? new Uint8Array(0);
  const isJson = isJsonContentType(resolveCreateContentType(options));

  if (isJson && data.length > 0) {
    data = validateJsonCreate(data, true);
  }

  const appendCount = data.length > 0 ? 1 : 0;
  const nextOffset = formatOffset(appendCount, data.length);

  return { data, appendCount, nextOffset };
};

export const prepareWholeValueCreate = (
  options: PutOptions,
  maxAppendBytes: number,
  maxStreamBytes: number
): PreparedWholeValueCreate => {
  const contentType = resolveCreateContentType(options);
  const prepared = prepareInitialData(options);
  assertPayloadSize(maxAppendBytes, prepared.data.length);
  assertPayloadSize(maxStreamBytes, prepared.data.length);

  return {
    contentType,
    ttlSeconds: options.ttlSeconds,
    expiresAt: options.expiresAt,
    closed: options.closed === true,
    data: prepared.data,
    appendCount: prepared.appendCount,
    nextOffset: prepared.nextOffset,
    appendEndPositions: prepared.data.length > 0 ? [prepared.data.length] : [],
  };
};

export const prepareWholeValueForkCreate = (
  source: WholeValueCreateSource,
  options: PutOptions,
  maxAppendBytes: number,
  maxStreamBytes: number
): PreparedWholeValueCreate => {
  validateAppendContentType(source.contentType, options.contentType);

  const forkOffset = options.forkOffset ?? source.nextOffset;
  const forkSubOffset = normalizeForkSubOffset(options.forkSubOffset);
  const { byteOffset: forkPos, tailPos: sourceTailPos } = validateReadOffset(
    forkOffset,
    source.nextOffset
  );
  assertPayloadSize(maxAppendBytes, forkPos);

  const prepared = prepareForkData(
    source.data,
    forkOffset,
    source.nextOffset,
    source.contentType,
    forkSubOffset,
    options.data
  );
  assertPayloadSize(maxAppendBytes, prepared.data.length);
  assertPayloadSize(maxStreamBytes, prepared.data.length);

  const createLength = preparedCreateDataLength(
    options.data,
    source.contentType
  );
  const prefixLength = prepared.data.length - createLength;
  const appendEndPositions = [
    ...(source.appendEndPositions ?? [sourceTailPos]),
  ].filter((position) => position <= prefixLength);
  if (prefixLength > 0 && appendEndPositions.at(-1) !== prefixLength) {
    appendEndPositions.push(prefixLength);
  }
  if (createLength > 0) {
    appendEndPositions.push(prepared.data.length);
  }

  return {
    contentType: source.contentType,
    ...inheritedExpiration(source, options),
    closed: false,
    forkOffset,
    forkSubOffset,
    data: prepared.data,
    appendCount: prepared.appendCount,
    nextOffset: prepared.nextOffset,
    appendEndPositions,
  };
};

const preparedCreateDataLength = (
  data: Uint8Array | undefined,
  contentType: string
): number => {
  if (data === undefined || data.length === 0) {
    return 0;
  }

  return isJsonContentType(contentType)
    ? prepareInitialData({ contentType, data }).data.length
    : data.length;
};

const jsonSubOffsetByteLength = (
  data: Uint8Array,
  subOffset: number
): number | null => {
  if (subOffset === 0) {
    return 0;
  }

  const text = new TextDecoder().decode(data);
  const json = text.endsWith(",") ? text.slice(0, -1) : text;

  try {
    const items = JSON.parse(`[${json}]`) as unknown[];
    if (subOffset > items.length) {
      return null;
    }
    const prefix = `${items
      .slice(0, subOffset)
      .map((item) => JSON.stringify(item))
      .join(",")},`;
    return new TextEncoder().encode(prefix).length;
  } catch {
    return null;
  }
};

const prepareForkPrefix = (
  sourceData: Uint8Array,
  forkOffset: Offset,
  sourceTailOffset: Offset,
  contentType: string,
  forkSubOffset: number | undefined
): PreparedData => {
  const { byteOffset } = validateReadOffset(forkOffset, sourceTailOffset);
  if (byteOffset > sourceData.length) {
    throw new InvalidOffsetError(forkOffset);
  }

  const parsedOffset = parseOffset(forkOffset);
  const subOffset = normalizeForkSubOffset(forkSubOffset);
  if (subOffset === undefined) {
    return {
      data: sourceData.slice(0, byteOffset),
      appendCount: parsedOffset?.seq ?? 0,
      nextOffset: forkOffset,
    };
  }

  const isJson = isJsonContentType(contentType);
  const subOffsetBytes = isJson
    ? jsonSubOffsetByteLength(sourceData.slice(byteOffset), subOffset)
    : subOffset;
  if (
    subOffsetBytes === null ||
    byteOffset + subOffsetBytes > sourceData.length
  ) {
    throw new InvalidOffsetError(forkOffset);
  }

  const data = sourceData.slice(0, byteOffset + subOffsetBytes);
  const appendCount = (parsedOffset?.seq ?? 0) + (isJson ? subOffset : 1);
  return {
    data,
    appendCount,
    nextOffset: formatOffset(appendCount, data.length),
  };
};

export const prepareForkData = (
  sourceData: Uint8Array,
  forkOffset: Offset,
  sourceTailOffset: Offset,
  contentType: string,
  forkSubOffset?: number,
  createData?: Uint8Array
): PreparedData => {
  const prepared = prepareForkPrefix(
    sourceData,
    forkOffset,
    sourceTailOffset,
    contentType,
    forkSubOffset
  );

  if (createData === undefined || createData.length === 0) {
    return prepared;
  }

  const data = isJsonContentType(contentType)
    ? validateJsonCreate(createData, true)
    : createData;
  if (data.length === 0) {
    return prepared;
  }

  const merged = concatenateData(prepared.data, data);
  const appendCount = prepared.appendCount + 1;
  return {
    data: merged,
    appendCount,
    nextOffset: formatOffset(appendCount, merged.length),
  };
};

export const validateAppendContentType = (
  streamContentType: string,
  requestContentType: string | undefined
): void => {
  if (!requestContentType) {
    return;
  }

  const streamNormalized = normalizeContentType(streamContentType);
  const reqNormalized = normalizeContentType(requestContentType);

  if (streamNormalized !== reqNormalized) {
    throw new ContentTypeMismatchError(streamNormalized, reqNormalized);
  }
};

export const validateAppendSeq = (
  lastSeq: string | undefined,
  requestSeq: string | undefined
): void => {
  if (requestSeq === undefined || lastSeq === undefined) {
    return;
  }

  if (requestSeq <= lastSeq) {
    throw new SequenceConflictError(`> ${lastSeq}`, requestSeq);
  }
};

export const mergeData = (
  existingData: Uint8Array,
  newData: Uint8Array,
  isJson: boolean
): Uint8Array => {
  if (isJson) {
    return processJsonAppend(existingData, newData);
  }

  const merged = new Uint8Array(existingData.length + newData.length);
  merged.set(existingData);
  merged.set(newData, existingData.length);
  return merged;
};

export const closedAppendResult = (
  path: string,
  incarnation: StreamIncarnation,
  nextOffset: Offset,
  closed: boolean,
  data: Uint8Array,
  options: AppendOptions | undefined,
  decision: ProducerAppendDecision
): AppendResult | undefined => {
  if (!closed) {
    return;
  }

  if (decision._tag === "Duplicate") {
    return {
      incarnation,
      nextOffset,
      producer: decision.result,
      closed: true,
      appended: false,
    };
  }

  if (options?.close === true && data.length === 0 && !options.producer) {
    return { incarnation, nextOffset, closed: true, appended: false };
  }

  throw new StreamClosedError(path, nextOffset);
};

export type PreparedAppend = {
  readonly data: Uint8Array;
  readonly appendCount: number;
  readonly nextOffset: Offset;
  readonly appended: boolean;
};

export const prepareAppendData = (
  existingData: Uint8Array,
  data: Uint8Array,
  contentType: string,
  appendCount: number,
  nextOffset: Offset
): PreparedAppend => {
  if (data.length === 0) {
    return { data: existingData, appendCount, nextOffset, appended: false };
  }

  const merged = mergeData(existingData, data, isJsonContentType(contentType));
  return {
    data: merged,
    appendCount: appendCount + 1,
    nextOffset: formatOffset(appendCount + 1, merged.length),
    appended: true,
  };
};

export const appendResult = (
  incarnation: StreamIncarnation,
  nextOffset: Offset,
  closed: boolean,
  appended: boolean,
  decision: ProducerAppendDecision
): AppendResult => ({
  incarnation,
  nextOffset,
  closed,
  appended,
  ...(decision._tag === "Accepted" ? { producer: decision.result } : {}),
});
