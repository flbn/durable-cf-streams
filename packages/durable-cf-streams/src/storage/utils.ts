import { formatOffset } from "../offsets.js";
import {
  isJsonContentType,
  normalizeContentType,
  processJsonAppend,
  validateJsonCreate,
} from "../protocol.js";

export type IdempotentCreateInfo = {
  readonly contentType?: string;
  readonly ttlSeconds?: number;
  readonly expiresAt?: string;
  readonly closed?: boolean;
};

export type CreateOptions = {
  readonly contentType?: string;
  readonly ttlSeconds?: number;
  readonly expiresAt?: string;
  readonly initialData?: Uint8Array;
  readonly closed?: boolean;
};

export const validateIdempotentCreate = (
  existing: IdempotentCreateInfo,
  options: CreateOptions
): void => {
  const existingNormalized = normalizeContentType(existing.contentType ?? "application/octet-stream");
  const reqNormalized = normalizeContentType(options.contentType ?? "application/octet-stream");

  const configMatch =
    existingNormalized === reqNormalized &&
    (options.ttlSeconds ?? undefined) === (existing.ttlSeconds ?? undefined) &&
    (options.expiresAt ?? undefined) === (existing.expiresAt ?? undefined) &&
    (options.closed ?? false) === (existing.closed ?? false);

  if (!configMatch) {
    throw new Error(
      `Stream already exists with different configuration: ${existing.contentType}`
    );
  }
};

export type PreparedData = {
  readonly data: Uint8Array;
  readonly appendCount: number;
  readonly nextOffset: string;
};

export const prepareInitialData = (options: CreateOptions): PreparedData => {
  let data = options.initialData ?? new Uint8Array(0);
  const isJson = isJsonContentType(options.contentType ?? "application/octet-stream");

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
    throw new Error(
      `Content-type mismatch: expected ${streamContentType}, got ${requestContentType}`
    );
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
    throw new Error(`Sequence conflict: ${requestSeq} <= ${lastSeq}`);
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
