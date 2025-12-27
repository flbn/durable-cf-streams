import { InvalidJsonError } from "./errors.js";
import type { Offset, StreamMetadata } from "./types.js";

export type ExpirationInfo = {
  readonly ttlSeconds?: number;
  readonly expiresAt?: string;
  readonly createdAt?: number;
};

export const isExpired = (info: ExpirationInfo): boolean => {
  const now = Date.now();

  if (info.expiresAt) {
    const expiresAtMs = new Date(info.expiresAt).getTime();
    if (Number.isNaN(expiresAtMs) || now >= expiresAtMs) {
      return true;
    }
  }

  if (info.ttlSeconds !== undefined && info.createdAt !== undefined) {
    const expiresAtMs = info.createdAt + info.ttlSeconds * 1000;
    if (now >= expiresAtMs) {
      return true;
    }
  }

  return false;
};

export const isMetadataExpired = (metadata: StreamMetadata): boolean =>
  isExpired(metadata);

const TTL_REGEX = /^[1-9][0-9]*$/;
const EXPIRES_AT_REGEX =
  /^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(\.\d+)?(Z|[+-]\d{2}:\d{2})$/;
const ETAG_REGEX = /^"([^:]+):([^:]+):([^"]+)"$/;

export const normalizeContentType = (contentType: string): string => {
  const semicolonIndex = contentType.indexOf(";");
  if (semicolonIndex !== -1) {
    return contentType.slice(0, semicolonIndex).trim().toLowerCase();
  }
  return contentType.trim().toLowerCase();
};

export const isJsonContentType = (contentType: string): boolean => {
  const normalized = normalizeContentType(contentType);
  return normalized === "application/json" || normalized.endsWith("+json");
};

export const validateTTL = (ttl: string): number | null => {
  if (!TTL_REGEX.test(ttl)) {
    return null;
  }
  const parsed = Number.parseInt(ttl, 10);
  return Number.isNaN(parsed) || parsed <= 0 ? null : parsed;
};

export const validateExpiresAt = (value: string): Date | null => {
  if (!EXPIRES_AT_REGEX.test(value)) {
    return null;
  }
  const date = new Date(value);
  return Number.isNaN(date.getTime()) ? null : date;
};

export const generateETag = (
  path: string,
  startOffset: Offset,
  endOffset: Offset
): string => {
  const pathBase64 = btoa(path);
  return `"${pathBase64}:${startOffset}:${endOffset}"`;
};

export const parseETag = (
  etag: string
): { path: string; startOffset: Offset; endOffset: Offset } | null => {
  const match = ETAG_REGEX.exec(etag);
  if (!match || match.length < 4) {
    return null;
  }

  const pathBase64 = match[1];
  const startOffset = match[2];
  const endOffset = match[3];

  if (!(pathBase64 && startOffset && endOffset)) {
    return null;
  }

  try {
    return {
      path: atob(pathBase64),
      startOffset,
      endOffset,
    };
  } catch {
    return null;
  }
};

export const processJsonAppend = (
  existing: Uint8Array,
  newData: Uint8Array
): Uint8Array => {
  const newStr = new TextDecoder().decode(newData).trim();
  let parsed: unknown;
  try {
    parsed = JSON.parse(newStr);
  } catch (e) {
    throw new InvalidJsonError(e instanceof Error ? e.message : "Invalid JSON");
  }

  const items: unknown[] = Array.isArray(parsed) ? parsed : [parsed];

  if (items.length === 0) {
    throw new InvalidJsonError("Empty array not allowed on append");
  }

  const serialized = `${items.map((item) => JSON.stringify(item)).join(",")},`;
  const serializedBytes = new TextEncoder().encode(serialized);

  const result = new Uint8Array(existing.length + serializedBytes.length);
  result.set(existing);
  result.set(serializedBytes, existing.length);

  return result;
};

export const formatJsonResponse = (data: Uint8Array): Uint8Array => {
  if (data.length === 0) {
    return new TextEncoder().encode("[]");
  }

  let str = new TextDecoder().decode(data);
  if (str.endsWith(",")) {
    str = str.slice(0, -1);
  }

  return new TextEncoder().encode(`[${str}]`);
};

export const validateJsonCreate = (
  data: Uint8Array,
  isPut: boolean
): Uint8Array => {
  const str = new TextDecoder().decode(data).trim();
  let parsed: unknown;
  try {
    parsed = JSON.parse(str);
  } catch (e) {
    throw new InvalidJsonError(e instanceof Error ? e.message : "Invalid JSON");
  }

  let items: unknown[];
  if (Array.isArray(parsed)) {
    if (parsed.length === 0 && !isPut) {
      throw new InvalidJsonError("Empty array not allowed on POST");
    }
    items = parsed;
  } else {
    items = [parsed];
  }

  if (items.length === 0) {
    return new Uint8Array(0);
  }

  const serialized = `${items.map((item) => JSON.stringify(item)).join(",")},`;
  return new TextEncoder().encode(serialized);
};
