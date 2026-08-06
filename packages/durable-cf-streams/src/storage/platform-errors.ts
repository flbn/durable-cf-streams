import { PayloadTooLargeError } from "../errors.js";

export const CLOUDFLARE_SQL_MAX_VALUE_BYTES = 2_000_000;

const SQL_PAYLOAD_TOO_LARGE_PATTERN =
  /SQLITE_TOOBIG|string or blob too big|row too big/i;

export const isSqlPayloadTooLargeError = (error: unknown): boolean => {
  if (!(error instanceof Error)) {
    return false;
  }

  return SQL_PAYLOAD_TOO_LARGE_PATTERN.test(error.message);
};

export const rethrowSqlPayloadTooLargeError = (
  error: unknown,
  receivedBytes: number
): never => {
  if (isSqlPayloadTooLargeError(error)) {
    throwSqlPayloadTooLargeError(receivedBytes);
  }

  throw error;
};

export const throwSqlPayloadTooLargeError = (receivedBytes: number): never => {
  throw new PayloadTooLargeError(CLOUDFLARE_SQL_MAX_VALUE_BYTES, receivedBytes);
};
