export const SQLITE_STREAMS_SCHEMA = `
  CREATE TABLE IF NOT EXISTS streams (
    path TEXT PRIMARY KEY,
    content_type TEXT NOT NULL,
    ttl_seconds INTEGER,
    expires_at TEXT,
    created_at INTEGER NOT NULL,
    last_accessed_at INTEGER,
    next_offset TEXT NOT NULL,
    last_seq TEXT,
    append_count INTEGER NOT NULL DEFAULT 0,
    closed INTEGER NOT NULL DEFAULT 0,
    forked_from TEXT,
    fork_offset TEXT,
    fork_sub_offset INTEGER,
    child_count INTEGER NOT NULL DEFAULT 0,
    deleted INTEGER NOT NULL DEFAULT 0
  )
`;

/**
 * initializes the stream metadata table used by `SqliteStore`.
 * NOTE: stream bytes must live in `stream_chunks`; a non-empty `streams.data` column is rejected because `SqliteStore` does not read snapshot bytes.
 */
export const initializeSqliteStreamsSchema = (sql: SqlStorage): void => {
  sql.exec(SQLITE_STREAMS_SCHEMA);
  const columns = sql.exec("PRAGMA table_info(streams)").toArray() as {
    name: string;
  }[];
  const hasColumn = (name: string) =>
    columns.some((column) => column.name === name);
  const addColumn = (name: string, sqlStatement: string) => {
    if (!hasColumn(name)) {
      sql.exec(sqlStatement);
    }
  };

  addColumn(
    "closed",
    "ALTER TABLE streams ADD COLUMN closed INTEGER NOT NULL DEFAULT 0"
  );
  addColumn(
    "last_accessed_at",
    "ALTER TABLE streams ADD COLUMN last_accessed_at INTEGER"
  );
  addColumn("forked_from", "ALTER TABLE streams ADD COLUMN forked_from TEXT");
  addColumn("fork_offset", "ALTER TABLE streams ADD COLUMN fork_offset TEXT");
  addColumn(
    "fork_sub_offset",
    "ALTER TABLE streams ADD COLUMN fork_sub_offset INTEGER"
  );
  addColumn(
    "child_count",
    "ALTER TABLE streams ADD COLUMN child_count INTEGER NOT NULL DEFAULT 0"
  );
  addColumn(
    "deleted",
    "ALTER TABLE streams ADD COLUMN deleted INTEGER NOT NULL DEFAULT 0"
  );

  if (hasColumn("data")) {
    const rows = sql
      .exec<{ row_count: number }>(
        "SELECT COUNT(*) AS row_count FROM streams WHERE length(data) > 0"
      )
      .toArray();
    if ((rows[0]?.row_count ?? 0) > 0) {
      throw new Error(
        "SqliteStore requires stream bytes in stream_chunks; found non-empty streams.data"
      );
    }
  }
};
