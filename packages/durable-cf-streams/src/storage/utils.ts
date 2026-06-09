import {
  ContentTypeMismatchError,
  InvalidOffsetError,
  SequenceConflictError,
  StreamClosedError,
  StreamConflictError,
  StreamGoneError,
} from "../errors.js";
import { formatOffset, offsetToBytePos, parseOffset } from "../offsets.js";
import type { ProducerAppendDecision } from "../producer.js";
import {
  isJsonContentType,
  normalizeContentType,
  processJsonAppend,
  validateJsonCreate,
} from "../protocol.js";
import type {
  AppendOptions,
  AppendResult,
  Offset,
  PutOptions,
} from "../types.js";

export type IdempotentCreateInfo = {
  readonly contentType: string;
  readonly ttlSeconds?: number;
  readonly expiresAt?: string;
  readonly closed?: boolean;
  readonly forkedFrom?: string;
  readonly forkOffset?: Offset;
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
  const reqNormalized = normalizeContentType(options.contentType);

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

export const prepareInitialData = (options: PutOptions): PreparedData => {
  let data = options.data ?? new Uint8Array(0);
  const isJson = isJsonContentType(options.contentType);

  if (isJson && data.length > 0) {
    data = validateJsonCreate(data, true);
  }

  const appendCount = data.length > 0 ? 1 : 0;
  const nextOffset = formatOffset(appendCount, data.length);

  return { data, appendCount, nextOffset };
};

export const prepareForkData = (
  sourceData: Uint8Array,
  forkOffset: Offset
): PreparedData => {
  const byteOffset = offsetToBytePos(forkOffset);
  if (byteOffset > sourceData.length) {
    throw new InvalidOffsetError(forkOffset);
  }

  const parsedOffset = parseOffset(forkOffset);
  return {
    data: sourceData.slice(0, byteOffset),
    appendCount: parsedOffset?.seq ?? 0,
    nextOffset: forkOffset,
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
      nextOffset,
      producer: decision.result,
      closed: true,
      appended: false,
    };
  }

  if (options?.close === true && data.length === 0 && !options.producer) {
    return { nextOffset, closed: true, appended: false };
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
  nextOffset: Offset,
  closed: boolean,
  appended: boolean,
  decision: ProducerAppendDecision
): AppendResult => ({
  nextOffset,
  closed,
  appended,
  ...(decision._tag === "Accepted" ? { producer: decision.result } : {}),
});
