import { Deferred, Effect } from "effect";
import { calculateCursor } from "../cursor.js";
import {
  PayloadTooLargeError,
  StreamConflictError,
  StreamNotFoundError,
} from "../errors.js";
import { formatOffset, initialOffset, offsetToBytePos } from "../offsets.js";
import { commitProducerAppend, evaluateProducerAppend } from "../producer.js";
import {
  formatJsonResponse,
  generateETag,
  isExpired,
  isJsonContentType,
  processJsonAppend,
} from "../protocol.js";
import { decodeProducerStateMapJson } from "../schema.js";
import type {
  AppendOptions,
  AppendResult,
  GetOptions,
  GetResult,
  HeadResult,
  Offset,
  PutOptions,
  PutResult,
  StreamMessage,
  WaitResult,
} from "../types.js";
import type { StreamStore } from "./interface.js";
import {
  CLOUDFLARE_SQL_MAX_VALUE_BYTES,
  rethrowSqlPayloadTooLargeError,
} from "./platform-errors.js";
import {
  appendResult,
  assertStreamLive,
  closedAppendResult,
  inheritedExpiration,
  normalizeForkSubOffset,
  prepareForkData,
  prepareInitialData,
  resolveCreateContentType,
  validateAppendContentType,
  validateAppendSeq,
  validateIdempotentCreate,
} from "./utils.js";
import { notifyDeletedWaiters, type Waiter, waitForChange } from "./waiters.js";

type StreamRow = {
  path: string;
  content_type: string;
  ttl_seconds: number | null;
  expires_at: string | null;
  created_at: number;
  last_accessed_at: number | null;
  next_offset: Offset;
  last_seq: string | null;
  producers: string;
  append_count: number;
  closed: number;
  forked_from: string | null;
  fork_offset: Offset | null;
  fork_sub_offset: number | null;
  child_count: number;
  deleted: number;
};

type ChunkRow = {
  start_pos: number;
  end_pos: number;
  start_offset: Offset;
  end_offset: Offset;
  data: ArrayBuffer;
};

type PreparedCreate = {
  readonly contentType: string;
  readonly ttlSeconds?: number;
  readonly expiresAt?: string;
  readonly data: Uint8Array;
  readonly appendCount: number;
  readonly nextOffset: Offset;
  readonly closed: boolean;
  readonly forkedFrom?: string;
  readonly forkOffset?: Offset;
  readonly forkSubOffset?: number;
};

type PreparedAppendChunk = {
  readonly data: Uint8Array;
  readonly appendCount: number;
  readonly nextOffset: Offset;
  readonly appended: boolean;
};

const D1_STREAMS_SCHEMA =
  "CREATE TABLE IF NOT EXISTS streams (path TEXT PRIMARY KEY, content_type TEXT NOT NULL, ttl_seconds INTEGER, expires_at TEXT, created_at INTEGER NOT NULL, last_accessed_at INTEGER, next_offset TEXT NOT NULL, last_seq TEXT, producers TEXT NOT NULL DEFAULT '{}', append_count INTEGER NOT NULL DEFAULT 0, closed INTEGER NOT NULL DEFAULT 0, forked_from TEXT, fork_offset TEXT, fork_sub_offset INTEGER, child_count INTEGER NOT NULL DEFAULT 0, deleted INTEGER NOT NULL DEFAULT 0);";

/**
 * initializes the stream metadata table used by `D1Store`.
 * NOTE: stream bytes must live in `stream_chunks`; a non-empty `streams.data` column is rejected because `D1Store` does not read snapshot bytes.
 */
const initializeD1StreamsSchema = async (db: D1Database): Promise<void> => {
  await db.exec(D1_STREAMS_SCHEMA);
  const columns = await db.prepare("PRAGMA table_info(streams)").all<{
    name: string;
  }>();
  const hasColumn = (name: string) =>
    columns.results.some((column) => column.name === name);
  const addColumn = async (name: string, sql: string) => {
    if (!hasColumn(name)) {
      await db.exec(sql);
    }
  };

  await addColumn(
    "closed",
    "ALTER TABLE streams ADD COLUMN closed INTEGER NOT NULL DEFAULT 0"
  );
  await addColumn(
    "last_accessed_at",
    "ALTER TABLE streams ADD COLUMN last_accessed_at INTEGER"
  );
  await addColumn(
    "forked_from",
    "ALTER TABLE streams ADD COLUMN forked_from TEXT"
  );
  await addColumn(
    "fork_offset",
    "ALTER TABLE streams ADD COLUMN fork_offset TEXT"
  );
  await addColumn(
    "fork_sub_offset",
    "ALTER TABLE streams ADD COLUMN fork_sub_offset INTEGER"
  );
  await addColumn(
    "child_count",
    "ALTER TABLE streams ADD COLUMN child_count INTEGER NOT NULL DEFAULT 0"
  );
  await addColumn(
    "deleted",
    "ALTER TABLE streams ADD COLUMN deleted INTEGER NOT NULL DEFAULT 0"
  );

  if (hasColumn("data")) {
    const rows = await db
      .prepare(
        "SELECT COUNT(*) AS row_count FROM streams WHERE length(data) > 0"
      )
      .all<{ row_count: number }>();
    if ((rows.results[0]?.row_count ?? 0) > 0) {
      throw new Error(
        "D1Store requires stream bytes in stream_chunks; found non-empty streams.data"
      );
    }
  }
};

export type D1StoreOptions = {
  /**
   * max bytes for one stored stream chunk.
   * NOTE: one append writes one chunk row, so keep this below Cloudflare's SQL row and BLOB ceiling.
   */
  readonly maxChunkBytes?: number;
};

export const DEFAULT_D1_MAX_CHUNK_BYTES = 1_000_000;

const isRowExpired = (row: {
  ttl_seconds: number | null;
  expires_at: string | null;
  created_at: number;
  last_accessed_at: number | null;
}): boolean =>
  isExpired({
    ttlSeconds: row.ttl_seconds ?? undefined,
    expiresAt: row.expires_at ?? undefined,
    createdAt: row.created_at,
    lastAccessedAt: row.last_accessed_at ?? undefined,
  });

const resolveMaxChunkBytes = (value: number | undefined): number => {
  const maxChunkBytes = value ?? DEFAULT_D1_MAX_CHUNK_BYTES;
  if (
    !Number.isSafeInteger(maxChunkBytes) ||
    maxChunkBytes <= 0 ||
    maxChunkBytes > CLOUDFLARE_SQL_MAX_VALUE_BYTES
  ) {
    throw new RangeError(
      `maxChunkBytes must be an integer between 1 and ${CLOUDFLARE_SQL_MAX_VALUE_BYTES}`
    );
  }

  return maxChunkBytes;
};

/**
 * d1 store backed by stream metadata rows and bounded append chunks.
 * NOTE: one append writes one bounded chunk row; callers that need larger single events should split them before append.
 */
export class D1Store implements StreamStore {
  private readonly db: D1Database;
  private readonly maxChunkBytes: number;
  private readonly waiters = new Map<string, Waiter[]>();
  private readonly streamCache = new Map<string, { contentType: string }>();

  private static chunkSchema =
    "CREATE TABLE IF NOT EXISTS stream_chunks (path TEXT NOT NULL, start_pos INTEGER NOT NULL, end_pos INTEGER NOT NULL, start_offset TEXT NOT NULL, end_offset TEXT NOT NULL, data BLOB NOT NULL, PRIMARY KEY (path, start_pos));";

  private static chunksByEndIndex =
    "CREATE INDEX IF NOT EXISTS stream_chunks_by_end ON stream_chunks(path, end_pos);";

  static schema = `${D1_STREAMS_SCHEMA}
${D1Store.chunkSchema}
${D1Store.chunksByEndIndex}`;

  constructor(db: D1Database, options?: D1StoreOptions) {
    this.db = db;
    this.maxChunkBytes = resolveMaxChunkBytes(options?.maxChunkBytes);
  }

  async initialize(): Promise<void> {
    await initializeD1StreamsSchema(this.db);
    await this.db.exec(D1Store.chunkSchema);
    await this.db.exec(D1Store.chunksByEndIndex);
  }

  private async touchStream(path: string, row: StreamRow): Promise<StreamRow> {
    if (row.ttl_seconds === null) {
      return row;
    }

    const lastAccessedAt = Date.now();
    await this.db
      .prepare("UPDATE streams SET last_accessed_at = ? WHERE path = ?")
      .bind(lastAccessedAt, path)
      .run();
    return { ...row, last_accessed_at: lastAccessedAt };
  }

  private async expireStream(
    path: string,
    row: StreamRow
  ): Promise<StreamRow | null> {
    if (row.child_count > 0) {
      await this.db
        .prepare("UPDATE streams SET deleted = 1 WHERE path = ?")
        .bind(path)
        .run();
      this.notifyDeleted(path);
      return { ...row, deleted: 1 };
    }

    await this.hardDelete(path, row);
    return null;
  }

  private async hardDelete(path: string, row: StreamRow): Promise<void> {
    this.notifyDeleted(path);
    await this.db.batch([
      this.db.prepare("DELETE FROM stream_chunks WHERE path = ?").bind(path),
      this.db.prepare("DELETE FROM streams WHERE path = ?").bind(path),
    ]);
    await this.releaseParent(row.forked_from ?? undefined);
  }

  private async releaseParent(parentPath: string | undefined): Promise<void> {
    if (!parentPath) {
      return;
    }

    const parent = await this.db
      .prepare("SELECT * FROM streams WHERE path = ?")
      .bind(parentPath)
      .first<StreamRow>();
    if (!parent) {
      return;
    }

    const childCount = Math.max(0, parent.child_count - 1);
    if (parent.deleted === 1 && childCount === 0) {
      await this.hardDelete(parentPath, { ...parent, child_count: childCount });
      return;
    }

    await this.db
      .prepare("UPDATE streams SET child_count = ? WHERE path = ?")
      .bind(childCount, parentPath)
      .run();
  }

  private async getStreamRow(path: string): Promise<StreamRow | null> {
    const row = await this.db
      .prepare("SELECT * FROM streams WHERE path = ?")
      .bind(path)
      .first<StreamRow>();

    if (!row) {
      return null;
    }

    if (isRowExpired(row)) {
      return await this.expireStream(path, row);
    }

    this.streamCache.set(path, { contentType: row.content_type });
    return row;
  }

  private async prepareCreate(options: PutOptions): Promise<PreparedCreate> {
    if (options.forkedFrom === undefined) {
      const prepared = prepareInitialData(options);
      return {
        ...prepared,
        contentType: resolveCreateContentType(options),
        ttlSeconds: options.ttlSeconds,
        expiresAt: options.expiresAt,
        closed: options.closed === true,
      };
    }

    return await this.prepareForkCreate(options, options.forkedFrom);
  }

  /**
   * copy a fork prefix into the child stream.
   * NOTE: linked parent chunks would save space, but v1 keeps delete and fork lifetime local by giving the child its own bytes.
   */
  private async prepareForkCreate(
    options: PutOptions,
    sourcePath: string
  ): Promise<PreparedCreate> {
    const source = await this.getStreamRow(sourcePath);
    if (!source) {
      throw new StreamNotFoundError(sourcePath);
    }
    if (source.deleted === 1) {
      throw new StreamConflictError("fork source is gone");
    }
    validateAppendContentType(source.content_type, options.contentType);

    const sourceData = await this.readBytes(sourcePath);
    const forkOffset = options.forkOffset ?? source.next_offset;
    const forkSubOffset = normalizeForkSubOffset(options.forkSubOffset);
    const prepared = prepareForkData(
      sourceData,
      forkOffset,
      source.content_type,
      forkSubOffset,
      options.data
    );
    const { ttlSeconds, expiresAt } = inheritedExpiration(
      {
        ttlSeconds: source.ttl_seconds ?? undefined,
        expiresAt: source.expires_at ?? undefined,
      },
      options
    );

    await this.db
      .prepare("UPDATE streams SET child_count = ? WHERE path = ?")
      .bind(source.child_count + 1, sourcePath)
      .run();

    return {
      ...prepared,
      contentType: source.content_type,
      ttlSeconds,
      expiresAt,
      closed: false,
      forkedFrom: sourcePath,
      forkOffset,
      forkSubOffset,
    };
  }

  private idempotentCreateResult(
    existing: StreamRow,
    options: PutOptions
  ): PutResult {
    if (existing.deleted === 1) {
      throw new StreamConflictError("stream is gone");
    }

    validateIdempotentCreate(
      {
        contentType: existing.content_type,
        ttlSeconds: existing.ttl_seconds ?? undefined,
        expiresAt: existing.expires_at ?? undefined,
        closed: existing.closed === 1,
        forkedFrom: existing.forked_from ?? undefined,
        forkOffset: existing.fork_offset ?? undefined,
        forkSubOffset: existing.fork_sub_offset ?? undefined,
      },
      options
    );

    return {
      created: false,
      nextOffset: existing.next_offset,
      contentType: existing.content_type,
      closed: existing.closed === 1,
    };
  }

  async put(path: string, options: PutOptions): Promise<PutResult> {
    const existing = await this.getStreamRow(path);

    if (existing) {
      return this.idempotentCreateResult(existing, options);
    }

    const prepared = await this.prepareCreate(options);
    const now = Date.now();
    try {
      await this.db.batch([
        this.db
          .prepare(
            `INSERT INTO streams (path, content_type, ttl_seconds, expires_at, created_at, last_accessed_at, next_offset, producers, append_count, closed, forked_from, fork_offset, fork_sub_offset, child_count, deleted)
             VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`
          )
          .bind(
            path,
            prepared.contentType,
            prepared.ttlSeconds ?? null,
            prepared.expiresAt ?? null,
            now,
            now,
            prepared.nextOffset,
            "{}",
            prepared.appendCount,
            prepared.closed ? 1 : 0,
            prepared.forkedFrom ?? null,
            prepared.forkOffset ?? null,
            prepared.forkSubOffset ?? null,
            0,
            0
          ),
        ...this.initialChunkStatements(
          path,
          prepared.data,
          prepared.nextOffset
        ),
      ]);
    } catch (error) {
      rethrowSqlPayloadTooLargeError(
        error,
        Math.min(prepared.data.length, this.maxChunkBytes)
      );
    }

    this.streamCache.set(path, { contentType: prepared.contentType });
    return {
      created: true,
      nextOffset: prepared.nextOffset,
      contentType: prepared.contentType,
      closed: prepared.closed,
    };
  }

  async append(
    path: string,
    data: Uint8Array,
    options?: AppendOptions
  ): Promise<AppendResult> {
    const stream = await this.getStreamRow(path);
    if (!stream) {
      throw new StreamNotFoundError(path);
    }
    assertStreamLive(path, { deleted: stream.deleted === 1 });

    const producers = decodeProducerStateMapJson(stream.producers);
    const producerDecision = evaluateProducerAppend(
      producers,
      options?.producer
    );
    const closedResult = closedAppendResult(
      path,
      stream.next_offset,
      stream.closed === 1,
      data,
      options,
      producerDecision
    );
    if (closedResult) {
      await this.touchStream(path, stream);
      return closedResult;
    }

    if (data.length > 0) {
      validateAppendContentType(stream.content_type, options?.contentType);
    }

    if (producerDecision._tag === "Duplicate") {
      await this.touchStream(path, stream);
      return {
        nextOffset: stream.next_offset,
        producer: producerDecision.result,
        closed: stream.closed === 1,
        appended: false,
      };
    }
    validateAppendSeq(stream.last_seq ?? undefined, options?.seq);

    const append = this.prepareAppendChunk(
      data,
      stream.content_type,
      stream.append_count,
      stream.next_offset
    );
    const touched = await this.touchStream(path, stream);
    const statements: D1PreparedStatement[] = [];

    if (append.appended) {
      const startPos = offsetToBytePos(stream.next_offset);
      statements.push(
        this.chunkInsertStatement(
          path,
          startPos,
          append.data,
          stream.next_offset,
          append.nextOffset
        )
      );
    }

    statements.push(
      this.db
        .prepare(
          "UPDATE streams SET next_offset = ?, append_count = ?, last_seq = ?, producers = ?, closed = ?, last_accessed_at = ? WHERE path = ?"
        )
        .bind(
          append.nextOffset,
          append.appendCount,
          options?.seq ?? stream.last_seq,
          JSON.stringify(commitProducerAppend(producers, producerDecision)),
          options?.close === true ? 1 : 0,
          touched.last_accessed_at,
          path
        )
    );

    try {
      await this.db.batch(statements);
    } catch (error) {
      rethrowSqlPayloadTooLargeError(error, append.data.length);
    }

    this.notifyWaiters(
      path,
      append.appended
        ? [
            {
              offset: stream.next_offset,
              timestamp: Date.now(),
              data: append.data,
            },
          ]
        : [],
      options?.close === true
    );

    return appendResult(
      append.nextOffset,
      options?.close === true,
      append.appended,
      producerDecision
    );
  }

  async get(path: string, options?: GetOptions): Promise<GetResult> {
    const stream = await this.getStreamRow(path);
    if (!stream) {
      throw new StreamNotFoundError(path);
    }
    assertStreamLive(path, { deleted: stream.deleted === 1 });
    const touched = await this.touchStream(path, stream);

    const startOffset = options?.offset ?? initialOffset();
    const messages = await this.readMessages(path, startOffset);

    return {
      messages,
      nextOffset: touched.next_offset,
      upToDate: true,
      cursor: calculateCursor(),
      etag: generateETag(path, startOffset, touched.next_offset),
      contentType: touched.content_type,
      closed: touched.closed === 1,
    };
  }

  async head(path: string): Promise<HeadResult | null> {
    const stream = await this.getStreamRow(path);
    if (!stream) {
      this.streamCache.delete(path);
      return null;
    }
    assertStreamLive(path, { deleted: stream.deleted === 1 });

    return {
      contentType: stream.content_type,
      nextOffset: stream.next_offset,
      etag: generateETag(path, initialOffset(), stream.next_offset),
      closed: stream.closed === 1,
      ttlSeconds: stream.ttl_seconds ?? undefined,
      expiresAt: stream.expires_at ?? undefined,
    };
  }

  async delete(path: string): Promise<void> {
    const stream = await this.getStreamRow(path);
    if (!stream) {
      return;
    }

    assertStreamLive(path, { deleted: stream.deleted === 1 });

    if (stream.child_count > 0) {
      await this.db
        .prepare("UPDATE streams SET deleted = 1 WHERE path = ?")
        .bind(path)
        .run();
      this.notifyDeleted(path);
      return;
    }

    await this.hardDelete(path, stream);
  }

  has(path: string): boolean {
    return this.streamCache.has(path);
  }

  async waitForData(
    path: string,
    offset: Offset,
    timeoutMs: number
  ): Promise<WaitResult> {
    const stream = await this.getStreamRow(path);
    if (!stream) {
      throw new StreamNotFoundError(path);
    }
    assertStreamLive(path, { deleted: stream.deleted === 1 });
    const touched = await this.touchStream(path, stream);

    const messages = await this.readMessages(path, offset);
    if (messages.length > 0) {
      return {
        messages,
        timedOut: false,
        closed: touched.closed === 1,
      };
    }

    if (touched.closed === 1) {
      return {
        messages: [],
        timedOut: false,
        closed: true,
      };
    }

    return waitForChange(
      {
        add: (waiter) => {
          const pathWaiters = this.waiters.get(path) ?? [];
          pathWaiters.push(waiter);
          this.waiters.set(path, pathWaiters);
        },
        remove: (waiter) => {
          const currentWaiters = this.waiters.get(path) ?? [];
          const index = currentWaiters.indexOf(waiter);
          if (index !== -1) {
            currentWaiters.splice(index, 1);
            this.waiters.set(path, currentWaiters);
          }
        },
      },
      offset,
      timeoutMs
    );
  }

  formatResponse(path: string, messages: StreamMessage[]): Uint8Array {
    const cached = this.streamCache.get(path);
    if (!cached) {
      return new Uint8Array(0);
    }

    if (messages.length === 0) {
      const isJson = isJsonContentType(cached.contentType);
      return isJson ? new TextEncoder().encode("[]") : new Uint8Array(0);
    }

    const combined = new Uint8Array(
      messages.reduce((acc, m) => acc + m.data.length, 0)
    );
    let offset = 0;
    for (const message of messages) {
      combined.set(message.data, offset);
      offset += message.data.length;
    }

    const isJson = isJsonContentType(cached.contentType);
    return isJson ? formatJsonResponse(combined) : combined;
  }

  private prepareAppendChunk(
    data: Uint8Array,
    contentType: string,
    appendCount: number,
    nextOffset: Offset
  ): PreparedAppendChunk {
    if (data.length === 0) {
      return { data, appendCount, nextOffset, appended: false };
    }

    const chunkData = isJsonContentType(contentType)
      ? processJsonAppend(new Uint8Array(0), data)
      : data;
    this.assertChunkSize(chunkData.length);

    const nextPos = offsetToBytePos(nextOffset) + chunkData.length;
    return {
      data: chunkData,
      appendCount: appendCount + 1,
      nextOffset: formatOffset(appendCount + 1, nextPos),
      appended: true,
    };
  }

  private assertChunkSize(size: number): void {
    if (size > this.maxChunkBytes) {
      throw new PayloadTooLargeError(this.maxChunkBytes, size);
    }
  }

  private initialChunkStatements(
    path: string,
    data: Uint8Array,
    finalOffset: Offset
  ): D1PreparedStatement[] {
    if (data.length === 0) {
      return [];
    }

    const statements: D1PreparedStatement[] = [];
    let startPos = 0;
    while (startPos < data.length) {
      const endPos = Math.min(startPos + this.maxChunkBytes, data.length);
      const chunk = data.slice(startPos, endPos);
      const endOffset =
        endPos === data.length ? finalOffset : formatOffset(0, endPos);
      statements.push(
        this.chunkInsertStatement(
          path,
          startPos,
          chunk,
          formatOffset(0, startPos),
          endOffset
        )
      );
      startPos = endPos;
    }

    return statements;
  }

  private chunkInsertStatement(
    path: string,
    startPos: number,
    data: Uint8Array,
    startOffset: Offset,
    endOffset: Offset
  ): D1PreparedStatement {
    this.assertChunkSize(data.length);
    return this.db
      .prepare(
        `INSERT INTO stream_chunks (path, start_pos, end_pos, start_offset, end_offset, data)
         VALUES (?, ?, ?, ?, ?, ?)`
      )
      .bind(
        path,
        startPos,
        startPos + data.length,
        startOffset,
        endOffset,
        data
      );
  }

  private async readBytes(path: string): Promise<Uint8Array> {
    const messages = await this.readMessages(path, initialOffset());
    const total = messages.reduce(
      (acc, message) => acc + message.data.length,
      0
    );
    const result = new Uint8Array(total);
    let offset = 0;
    for (const message of messages) {
      result.set(message.data, offset);
      offset += message.data.length;
    }
    return result;
  }

  private async readMessages(
    path: string,
    startOffset: Offset
  ): Promise<StreamMessage[]> {
    const startPos = offsetToBytePos(startOffset);
    const messages: StreamMessage[] = [];

    for (const chunk of await this.readChunkRows(path, startPos)) {
      const chunkData = new Uint8Array(chunk.data);
      const messageStart = Math.max(startPos, chunk.start_pos);
      if (messageStart >= chunk.end_pos) {
        continue;
      }

      let messageOffset = startOffset;
      if (messageStart !== startPos) {
        messageOffset =
          messageStart === chunk.start_pos
            ? chunk.start_offset
            : formatOffset(0, messageStart);
      }

      messages.push({
        offset: messageOffset,
        timestamp: Date.now(),
        data: chunkData.slice(messageStart - chunk.start_pos),
      });
    }

    return messages;
  }

  private async readChunkRows(
    path: string,
    startPos: number
  ): Promise<ChunkRow[]> {
    const result = await this.db
      .prepare(
        `SELECT start_pos, end_pos, start_offset, end_offset, data
         FROM stream_chunks
         WHERE path = ? AND end_pos > ?
         ORDER BY start_pos`
      )
      .bind(path, startPos)
      .all<ChunkRow>();

    return result.results;
  }

  private notifyWaiters(
    path: string,
    messages: readonly StreamMessage[],
    closed = false
  ): void {
    const waiters = this.waiters.get(path) ?? [];
    this.waiters.set(path, []);

    const effect = Effect.forEach(waiters, (waiter) => {
      const available = this.messagesForWaiter(waiter, messages);

      if (available.length > 0 || closed) {
        return Deferred.succeed(waiter.deferred, {
          messages: available,
          timedOut: false,
          closed,
        });
      }

      return Effect.sync(() => {
        const remaining = this.waiters.get(path) ?? [];
        remaining.push(waiter);
        this.waiters.set(path, remaining);
      });
    });

    Effect.runSync(effect);
  }

  private messagesForWaiter(
    waiter: Waiter,
    messages: readonly StreamMessage[]
  ): StreamMessage[] {
    const byteOffset = offsetToBytePos(waiter.offset);
    const available: StreamMessage[] = [];

    for (const message of messages) {
      const startPos = offsetToBytePos(message.offset);
      const endPos = startPos + message.data.length;
      if (byteOffset >= endPos) {
        continue;
      }

      const sliceStart = Math.max(0, byteOffset - startPos);
      available.push({
        offset: byteOffset > startPos ? waiter.offset : message.offset,
        timestamp: Date.now(),
        data: message.data.slice(sliceStart),
      });
    }

    return available;
  }

  private notifyDeleted(path: string): void {
    const waiters = this.waiters.get(path) ?? [];
    notifyDeletedWaiters(waiters);

    this.waiters.delete(path);
    this.streamCache.delete(path);
  }
}
