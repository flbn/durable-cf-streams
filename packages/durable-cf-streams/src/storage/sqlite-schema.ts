export const SQLITE_STREAMS_SCHEMA = `
  CREATE TABLE IF NOT EXISTS streams (
    path TEXT PRIMARY KEY,
    incarnation TEXT NOT NULL,
    content_type TEXT NOT NULL,
    ttl_seconds INTEGER,
    expires_at TEXT,
    created_at INTEGER NOT NULL,
    last_accessed_at INTEGER,
    next_offset TEXT NOT NULL,
    last_seq TEXT,
    producer_id TEXT,
    producer_epoch INTEGER NOT NULL DEFAULT 0,
    next_producer_sequence INTEGER NOT NULL DEFAULT 0,
    append_count INTEGER NOT NULL DEFAULT 0,
    closed INTEGER NOT NULL DEFAULT 0,
    forked_from TEXT,
    fork_offset TEXT,
    fork_sub_offset INTEGER,
    child_count INTEGER NOT NULL DEFAULT 0,
    deleted INTEGER NOT NULL DEFAULT 0
  )
`;

export const SQL_STREAMS_FORMAT_VERSION = "4";

export const SQL_STREAMS_META_SCHEMA =
  "CREATE TABLE IF NOT EXISTS stream_meta (key TEXT PRIMARY KEY, value TEXT NOT NULL);";

/**
 * creates the stream metadata table used by `SqliteStore`.
 * NOTE: this is a breaking schema; unversioned SQL stores are rejected instead of migrated.
 */
export const initializeSqliteStreamsSchema = (sql: SqlStorage): void => {
  sql.exec(SQL_STREAMS_META_SCHEMA);
  const stored = sql
    .exec("SELECT value FROM stream_meta WHERE key = 'format_version'")
    .toArray()[0]?.value;
  if (stored !== undefined && String(stored) !== SQL_STREAMS_FORMAT_VERSION) {
    throw new Error(
      `Unsupported stream storage format ${String(stored)}; expected ${SQL_STREAMS_FORMAT_VERSION}`
    );
  }
  if (stored === undefined) {
    const existing = sql
      .exec(
        "SELECT name FROM sqlite_master WHERE type = 'table' AND name IN ('streams', 'stream_chunks', 'stream_producers') LIMIT 1"
      )
      .toArray()[0];
    if (existing !== undefined) {
      throw new Error(
        "Unsupported unversioned stream storage format; clear the old SQL stream tables before opening this breaking schema"
      );
    }
    sql.exec(
      "INSERT INTO stream_meta (key, value) VALUES ('format_version', ?)",
      SQL_STREAMS_FORMAT_VERSION
    );
  }
  sql.exec(SQLITE_STREAMS_SCHEMA);
};
