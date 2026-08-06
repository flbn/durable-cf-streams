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
import { SqliteStore } from "./sqlite.js";
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
  data: ArrayBuffer;
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

export type ChunkedSqliteStoreOptions = {
  /**
   * max bytes for one stored stream chunk.
   * NOTE: one append writes one chunk row, so keep this below Cloudflare's SQL row and BLOB ceiling.
   */
  readonly maxChunkBytes?: number;
};

export const DEFAULT_CHUNKED_SQLITE_MAX_CHUNK_BYTES = 1_000_000;

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
  const maxChunkBytes = value ?? DEFAULT_CHUNKED_SQLITE_MAX_CHUNK_BYTES;
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
 * opt-in SQLite store for streams that can outgrow one Cloudflare SQL row.
 * NOTE: one append writes one bounded chunk row; callers that need larger single events should split them before append.
 * NOTE: old `streams.data` bytes stay as a legacy prefix so enabling this adapter does not rewrite existing streams.
 */
export class ChunkedSqliteStore implements StreamStore {
  private readonly sql: SqlStorage;
  private readonly maxChunkBytes: number;
  private readonly waiters = new Map<string, Waiter[]>();
  private readonly streamCache = new Map<string, { contentType: string }>();

  static schema = `
    CREATE TABLE IF NOT EXISTS stream_chunks (
      path TEXT NOT NULL,
      start_pos INTEGER NOT NULL,
      end_pos INTEGER NOT NULL,
      start_offset TEXT NOT NULL,
      end_offset TEXT NOT NULL,
      data BLOB NOT NULL,
      PRIMARY KEY (path, start_pos)
    );
    CREATE INDEX IF NOT EXISTS stream_chunks_by_end
      ON stream_chunks(path, end_pos);
  `;

  constructor(sql: SqlStorage, options?: ChunkedSqliteStoreOptions) {
    this.sql = sql;
    this.maxChunkBytes = resolveMaxChunkBytes(options?.maxChunkBytes);
  }

  initialize(): void {
    new SqliteStore(this.sql).initialize();
    this.sql.exec(ChunkedSqliteStore.schema);
  }

  private touchStream(path: string, row: StreamRow): StreamRow {
    if (row.ttl_seconds === null) {
      return row;
    }

    const lastAccessedAt = Date.now();
    this.sql.exec(
      "UPDATE streams SET last_accessed_at = ? WHERE path = ?",
      lastAccessedAt,
      path
    );
    return { ...row, last_accessed_at: lastAccessedAt };
  }

  private expireStream(path: string, row: StreamRow): StreamRow | null {
    if (row.child_count > 0) {
      this.sql.exec("UPDATE streams SET deleted = 1 WHERE path = ?", path);
      this.notifyDeleted(path);
      return { ...row, deleted: 1 };
    }

    this.hardDelete(path, row);
    return null;
  }

  private hardDelete(path: string, row: StreamRow): void {
    this.notifyDeleted(path);
    this.sql.exec("DELETE FROM stream_chunks WHERE path = ?", path);
    this.sql.exec("DELETE FROM streams WHERE path = ?", path);
    this.releaseParent(row.forked_from ?? undefined);
  }

  private releaseParent(parentPath: string | undefined): void {
    if (!parentPath) {
      return;
    }

    const rows = this.sql
      .exec("SELECT * FROM streams WHERE path = ?", parentPath)
      .toArray() as StreamRow[];
    if (rows.length === 0) {
      return;
    }

    const parent = rows[0] as StreamRow;
    const childCount = Math.max(0, parent.child_count - 1);
    if (parent.deleted === 1 && childCount === 0) {
      this.hardDelete(parentPath, { ...parent, child_count: childCount });
      return;
    }

    this.sql.exec(
      "UPDATE streams SET child_count = ? WHERE path = ?",
      childCount,
      parentPath
    );
  }

  private getStreamRow(path: string): StreamRow | null {
    const rows = this.sql
      .exec("SELECT * FROM streams WHERE path = ?", path)
      .toArray() as StreamRow[];

    if (rows.length === 0) {
      return null;
    }

    const row = rows[0] as StreamRow;
    if (isRowExpired(row)) {
      return this.expireStream(path, row);
    }

    this.streamCache.set(path, { contentType: row.content_type });
    return row;
  }

  private prepareCreate(options: PutOptions): PreparedCreate {
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

    return this.prepareForkCreate(options, options.forkedFrom);
  }

  /**
   * copy a fork prefix into the child stream.
   * NOTE: linked parent chunks would save space, but v1 keeps delete and fork lifetime local by giving the child its own bytes.
   */
  private prepareForkCreate(
    options: PutOptions,
    sourcePath: string
  ): PreparedCreate {
    const source = this.getStreamRow(sourcePath);
    if (!source) {
      throw new StreamNotFoundError(sourcePath);
    }
    if (source.deleted === 1) {
      throw new StreamConflictError("fork source is gone");
    }
    validateAppendContentType(source.content_type, options.contentType);

    const sourceData = this.readBytes(sourcePath, source);
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

    this.sql.exec(
      "UPDATE streams SET child_count = ? WHERE path = ?",
      source.child_count + 1,
      sourcePath
    );

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

  put(path: string, options: PutOptions): Promise<PutResult> {
    const existing = this.getStreamRow(path);

    if (existing) {
      return Promise.resolve(this.idempotentCreateResult(existing, options));
    }

    const prepared = this.prepareCreate(options);
    const now = Date.now();
    this.sql.exec(
      `INSERT INTO streams (path, content_type, ttl_seconds, expires_at, created_at, last_accessed_at, data, next_offset, producers, append_count, closed, forked_from, fork_offset, fork_sub_offset, child_count, deleted)
       VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
      path,
      prepared.contentType,
      prepared.ttlSeconds ?? null,
      prepared.expiresAt ?? null,
      now,
      now,
      new Uint8Array(0),
      prepared.nextOffset,
      "{}",
      prepared.appendCount,
      prepared.closed ? 1 : 0,
      prepared.forkedFrom ?? null,
      prepared.forkOffset ?? null,
      prepared.forkSubOffset ?? null,
      0,
      0
    );
    this.insertInitialChunks(path, prepared.data, prepared.nextOffset);

    this.streamCache.set(path, { contentType: prepared.contentType });
    return Promise.resolve({
      created: true,
      nextOffset: prepared.nextOffset,
      contentType: prepared.contentType,
      closed: prepared.closed,
    });
  }

  append(
    path: string,
    data: Uint8Array,
    options?: AppendOptions
  ): Promise<AppendResult> {
    const stream = this.getStreamRow(path);
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
      this.touchStream(path, stream);
      return Promise.resolve(closedResult);
    }

    if (data.length > 0) {
      validateAppendContentType(stream.content_type, options?.contentType);
    }

    if (producerDecision._tag === "Duplicate") {
      this.touchStream(path, stream);
      return Promise.resolve({
        nextOffset: stream.next_offset,
        producer: producerDecision.result,
        closed: stream.closed === 1,
        appended: false,
      });
    }
    validateAppendSeq(stream.last_seq ?? undefined, options?.seq);

    const append = this.prepareAppendChunk(
      data,
      stream.content_type,
      stream.append_count,
      stream.next_offset
    );
    const touched = this.touchStream(path, stream);

    if (append.appended) {
      const startPos = offsetToBytePos(stream.next_offset);
      this.insertChunk(
        path,
        startPos,
        append.data,
        stream.next_offset,
        append.nextOffset
      );
    }

    this.sql.exec(
      "UPDATE streams SET next_offset = ?, append_count = ?, last_seq = ?, producers = ?, closed = ?, last_accessed_at = ? WHERE path = ?",
      append.nextOffset,
      append.appendCount,
      options?.seq ?? stream.last_seq,
      JSON.stringify(commitProducerAppend(producers, producerDecision)),
      options?.close === true ? 1 : 0,
      touched.last_accessed_at,
      path
    );

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

    return Promise.resolve(
      appendResult(
        append.nextOffset,
        options?.close === true,
        append.appended,
        producerDecision
      )
    );
  }

  get(path: string, options?: GetOptions): Promise<GetResult> {
    const stream = this.getStreamRow(path);
    if (!stream) {
      throw new StreamNotFoundError(path);
    }
    assertStreamLive(path, { deleted: stream.deleted === 1 });
    const touched = this.touchStream(path, stream);

    const startOffset = options?.offset ?? initialOffset();
    const messages = this.readMessages(path, touched, startOffset);

    return Promise.resolve({
      messages,
      nextOffset: touched.next_offset,
      upToDate: true,
      cursor: calculateCursor(),
      etag: generateETag(path, startOffset, touched.next_offset),
      contentType: touched.content_type,
      closed: touched.closed === 1,
    });
  }

  head(path: string): Promise<HeadResult | null> {
    const stream = this.getStreamRow(path);
    if (!stream) {
      return Promise.resolve(null);
    }
    assertStreamLive(path, { deleted: stream.deleted === 1 });

    return Promise.resolve({
      contentType: stream.content_type,
      nextOffset: stream.next_offset,
      etag: generateETag(path, initialOffset(), stream.next_offset),
      closed: stream.closed === 1,
      ttlSeconds: stream.ttl_seconds ?? undefined,
      expiresAt: stream.expires_at ?? undefined,
    });
  }

  delete(path: string): Promise<void> {
    const stream = this.getStreamRow(path);
    if (!stream) {
      return Promise.resolve();
    }

    assertStreamLive(path, { deleted: stream.deleted === 1 });

    if (stream.child_count > 0) {
      this.sql.exec("UPDATE streams SET deleted = 1 WHERE path = ?", path);
      this.notifyDeleted(path);
      return Promise.resolve();
    }

    this.hardDelete(path, stream);
    return Promise.resolve();
  }

  has(path: string): boolean {
    const stream = this.getStreamRow(path);
    return stream !== null && stream.deleted !== 1;
  }

  waitForData(
    path: string,
    offset: Offset,
    timeoutMs: number
  ): Promise<WaitResult> {
    const stream = this.getStreamRow(path);
    if (!stream) {
      throw new StreamNotFoundError(path);
    }
    assertStreamLive(path, { deleted: stream.deleted === 1 });
    const touched = this.touchStream(path, stream);

    const messages = this.readMessages(path, touched, offset);
    if (messages.length > 0) {
      return Promise.resolve({
        messages,
        timedOut: false,
        closed: touched.closed === 1,
      });
    }

    if (touched.closed === 1) {
      return Promise.resolve({
        messages: [],
        timedOut: false,
        closed: true,
      });
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

  private insertInitialChunks(
    path: string,
    data: Uint8Array,
    finalOffset: Offset
  ): void {
    if (data.length === 0) {
      return;
    }

    let startPos = 0;
    while (startPos < data.length) {
      const endPos = Math.min(startPos + this.maxChunkBytes, data.length);
      const chunk = data.slice(startPos, endPos);
      const endOffset =
        endPos === data.length ? finalOffset : formatOffset(0, endPos);
      this.insertChunk(
        path,
        startPos,
        chunk,
        formatOffset(0, startPos),
        endOffset
      );
      startPos = endPos;
    }
  }

  private insertChunk(
    path: string,
    startPos: number,
    data: Uint8Array,
    startOffset: Offset,
    endOffset: Offset
  ): void {
    this.assertChunkSize(data.length);
    try {
      this.sql.exec(
        `INSERT INTO stream_chunks (path, start_pos, end_pos, start_offset, end_offset, data)
         VALUES (?, ?, ?, ?, ?, ?)`,
        path,
        startPos,
        startPos + data.length,
        startOffset,
        endOffset,
        data
      );
    } catch (error) {
      rethrowSqlPayloadTooLargeError(error, data.length);
    }
  }

  private readBytes(path: string, stream: StreamRow): Uint8Array {
    const messages = this.readMessages(path, stream, initialOffset());
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

  /**
   * read old snapshot bytes before chunk rows.
   * NOTE: this is compatibility, not migration; new appends still go to `stream_chunks`.
   */
  private readMessages(
    path: string,
    stream: StreamRow,
    startOffset: Offset
  ): StreamMessage[] {
    const startPos = offsetToBytePos(startOffset);
    const legacyData = new Uint8Array(stream.data);
    const messages: StreamMessage[] = [];

    if (startPos < legacyData.length) {
      messages.push({
        offset: startOffset,
        timestamp: Date.now(),
        data: legacyData.slice(startPos),
      });
    }

    for (const chunk of this.readChunkRows(path, startPos)) {
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

  private readChunkRows(path: string, startPos: number): ChunkRow[] {
    return this.sql
      .exec(
        `SELECT start_pos, end_pos, start_offset, end_offset, data
         FROM stream_chunks
         WHERE path = ? AND end_pos > ?
         ORDER BY start_pos`,
        path,
        startPos
      )
      .toArray() as ChunkRow[];
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
