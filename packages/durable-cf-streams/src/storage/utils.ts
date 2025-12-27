import {
  ContentTypeMismatchError,
  SequenceConflictError,
  StreamConflictError,
} from "../errors.js";
import { formatOffset } from "../offsets.js";
import {
  isJsonContentType,
  normalizeContentType,
  processJsonAppend,
  validateJsonCreate,
} from "../protocol.js";
import type { PutOptions } from "../types.js";

export type IdempotentCreateInfo = {
  readonly contentType: string;
  readonly ttlSeconds?: number;
  readonly expiresAt?: string;
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

  if (options.ttlSeconds !== existing.ttlSeconds) {
    throw new StreamConflictError("TTL mismatch on idempotent create");
  }

  if (options.expiresAt !== existing.expiresAt) {
    throw new StreamConflictError("Expires-At mismatch on idempotent create");
  }
};

export type PreparedData = {
  readonly data: Uint8Array;
  readonly appendCount: number;
  readonly nextOffset: string;
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
