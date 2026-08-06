import {
  ChunkedSqliteStore,
  type ChunkedSqliteStoreOptions,
  DEFAULT_CHUNKED_SQLITE_MAX_CHUNK_BYTES,
} from "./chunked-sqlite.js";
import { SQLITE_STREAMS_SCHEMA } from "./sqlite-schema.js";

export type SqliteStoreOptions = ChunkedSqliteStoreOptions;

export const DEFAULT_SQLITE_MAX_CHUNK_BYTES =
  DEFAULT_CHUNKED_SQLITE_MAX_CHUNK_BYTES;

/**
 * default sqlite store for durable object storage.
 * NOTE: appends use bounded chunk rows by default while old snapshot bytes stay readable.
 */
export class SqliteStore extends ChunkedSqliteStore {
  static schema = `${SQLITE_STREAMS_SCHEMA};
${ChunkedSqliteStore.schema}`;
}
