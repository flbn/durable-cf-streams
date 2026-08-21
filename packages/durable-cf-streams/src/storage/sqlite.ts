import { Deferred, Effect } from "effect";
import { calculateCursor } from "../cursor.js";
import {
  InvalidOffsetError,
  InvalidProducerError,
  PayloadTooLargeError,
  StreamClosedError,
  StreamConflictError,
  StreamNotFoundError,
} from "../errors.js";
import { formatOffset, initialOffset, offsetToBytePos } from "../offsets.js";
import {
  evaluateClaimedProducerAppend,
  evaluateProducerAppend,
  type ProducerAppendDecision,
} from "../producer.js";
import {
  formatJsonResponse,
  generateETag,
  isExpired,
  isJsonContentType,
  processJsonAppend,
} from "../protocol.js";
import type {
  AppendOptions,
  AppendResult,
  GetOptions,
  GetResult,
  HeadResult,
  Offset,
  ProducerClaim,
  ProducerState,
  PutOptions,
  PutResult,
  StreamIncarnation,
  StreamMessage,
  WaitOptions,
  WaitResult,
} from "../types.js";
import type { StreamStore } from "./interface.js";
import {
  CLOUDFLARE_SQL_MAX_VALUE_BYTES,
  rethrowSqlPayloadTooLargeError,
} from "./platform-errors.js";
import {
  readSqlChunkMessages,
  type SqlChunkMessagesResult,
  type SqlChunkRow,
} from "./sql-chunks.js";
import {
  initializeSqliteStreamsSchema,
  SQL_STREAMS_META_SCHEMA,
  SQLITE_STREAMS_SCHEMA,
} from "./sqlite-schema.js";
import {
  appendResult,
  assertStreamIncarnation,
  assertStreamLive,
  closedAppendResult,
  generateIncarnation,
  inheritedExpiration,
  normalizeForkSubOffset,
  prepareForkData,
  prepareInitialData,
  resolveCreateContentType,
  validateAppendContentType,
  validateAppendSeq,
  validateIdempotentCreate,
  validateReadOffset,
} from "./utils.js";
import { notifyDeletedWaiters, type Waiter, waitForChange } from "./waiters.js";

type StreamRow = {
  path: string;
  incarnation: StreamIncarnation;
  content_type: string;
  ttl_seconds: number | null;
  expires_at: string | null;
  created_at: number;
  last_accessed_at: number | null;
  next_offset: Offset;
  last_seq: string | null;
  producer_id: string | null;
  producer_epoch: number;
  next_producer_sequence: number;
  append_count: number;
  closed: number;
  forked_from: string | null;
  fork_offset: Offset | null;
  fork_sub_offset: number | null;
  child_count: number;
  deleted: number;
};

type ChunkRow = SqlChunkRow;

type ProducerRow = {
  epoch: number;
  seq: number;
};

type ProducerAppendRow = {
  start_offset: Offset;
  end_offset: Offset;
  data_length: number;
  closed: number;
};

type PreparedCreate = {
  readonly contentType: string;
  readonly incarnation: StreamIncarnation;
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

type ReadWindow = {
  readonly messages: StreamMessage[];
  readonly nextOffset: Offset;
  readonly upToDate: boolean;
};

export type SqliteStoreOptions = {
  /**
   * max bytes for one stored stream chunk.
   * NOTE: one append may spill into multiple chunk rows; each stored row stays below Cloudflare's SQL row and BLOB ceiling.
   */
  readonly maxChunkBytes?: number;
  /**
   * max stream bytes returned from one read.
   * NOTE: reads stop before the durable tail when the window fills; resume from the returned `nextOffset` until `upToDate` is true.
   */
  readonly maxReadBytes?: number;
  /**
   * max logical bytes accepted by one put or append before storage chunking.
   * NOTE: this bounds one API call; each stored chunk still stays under `maxChunkBytes`.
   * NOTE: `maxAppendBytes / maxChunkBytes` must stay near the default chunk count so one logical write cannot create an unbounded sql transaction.
   */
  readonly maxAppendBytes?: number;
};

export const DEFAULT_SQLITE_MAX_CHUNK_BYTES = 512 * 1024;
export const DEFAULT_SQLITE_MAX_READ_BYTES = DEFAULT_SQLITE_MAX_CHUNK_BYTES;
/**
 * default ceiling for one put, fork result, or append before it reaches SQL.
 * NOTE: this mirrors the 12 MiB batch ceiling used upstream; larger tool or message output belongs upstream of persistence.
 */
export const DEFAULT_SQLITE_MAX_APPEND_BYTES = 12 * 1024 * 1024;
const MAX_SQLITE_CHUNKS_PER_WRITE = Math.ceil(
  DEFAULT_SQLITE_MAX_APPEND_BYTES / DEFAULT_SQLITE_MAX_CHUNK_BYTES
);

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

const resolveMaxSqlBytes = (name: string, value: number): number => {
  if (
    !Number.isSafeInteger(value) ||
    value <= 0 ||
    value > CLOUDFLARE_SQL_MAX_VALUE_BYTES
  ) {
    throw new RangeError(
      `${name} must be an integer between 1 and ${CLOUDFLARE_SQL_MAX_VALUE_BYTES}`
    );
  }

  return value;
};

const resolveMaxLogicalBytes = (
  name: string,
  value: number,
  maxChunkBytes: number
): number => {
  if (!Number.isSafeInteger(value) || value <= 0) {
    throw new RangeError(`${name} must be a positive safe integer`);
  }
  if (Math.ceil(value / maxChunkBytes) > MAX_SQLITE_CHUNKS_PER_WRITE) {
    throw new RangeError(
      `${name} would create more than ${MAX_SQLITE_CHUNKS_PER_WRITE} SQL chunks per write`
    );
  }

  return value;
};

const bytesEqual = (left: Uint8Array, right: Uint8Array): boolean => {
  if (left.length !== right.length) {
    return false;
  }
  for (let index = 0; index < left.length; index += 1) {
    if (left[index] !== right[index]) {
      return false;
    }
  }
  return true;
};

/**
 * sqlite store backed by stream metadata rows, keyed producer state rows, and bounded append chunks.
 * NOTE: producer idempotency state lives in `stream_producers`, keeping the stream metadata row bounded.
 * NOTE: one append may spill into multiple bounded chunk rows while keeping one logical append index.
 */
export class SqliteStore implements StreamStore {
  private readonly storage: DurableObjectStorage;
  private readonly sql: SqlStorage;
  private readonly maxChunkBytes: number;
  private readonly maxReadBytes: number;
  private readonly maxAppendBytes: number;
  private readonly waiters = new Map<string, Waiter[]>();
  private readonly streamCache = new Map<string, { contentType: string }>();

  private static chunkSchema = `
    CREATE TABLE IF NOT EXISTS stream_chunks (
      path TEXT NOT NULL,
      incarnation TEXT NOT NULL,
      append_index INTEGER NOT NULL,
      chunk_index INTEGER NOT NULL CHECK (chunk_index >= 0),
      chunk_count INTEGER NOT NULL CHECK (chunk_count > 0),
      start_pos INTEGER NOT NULL,
      end_pos INTEGER NOT NULL,
      start_offset TEXT NOT NULL,
      end_offset TEXT NOT NULL,
      data BLOB NOT NULL,
      PRIMARY KEY (path, incarnation, start_pos)
    );
    CREATE INDEX IF NOT EXISTS stream_chunks_by_end
      ON stream_chunks(path, incarnation, end_pos);
    CREATE INDEX IF NOT EXISTS stream_chunks_by_append
      ON stream_chunks(path, incarnation, append_index, chunk_index);
  `;

  private static producerSchema = `
    CREATE TABLE IF NOT EXISTS stream_producers (
      path TEXT NOT NULL,
      incarnation TEXT NOT NULL,
      producer_id TEXT NOT NULL,
      epoch INTEGER NOT NULL,
      seq INTEGER NOT NULL,
      PRIMARY KEY (path, incarnation, producer_id)
    );
  `;

  private static producerAppendSchema = `
    CREATE TABLE IF NOT EXISTS stream_producer_appends (
      path TEXT NOT NULL,
      incarnation TEXT NOT NULL,
      producer_id TEXT NOT NULL,
      epoch INTEGER NOT NULL,
      seq INTEGER NOT NULL,
      append_index INTEGER NOT NULL,
      start_offset TEXT NOT NULL,
      end_offset TEXT NOT NULL,
      data_length INTEGER NOT NULL,
      closed INTEGER NOT NULL DEFAULT 0,
      PRIMARY KEY (path, incarnation, producer_id, epoch, seq)
    );
  `;

  static schema = `${SQL_STREAMS_META_SCHEMA};
${SQLITE_STREAMS_SCHEMA};
${SqliteStore.chunkSchema}
${SqliteStore.producerSchema}
${SqliteStore.producerAppendSchema}`;

  constructor(storage: DurableObjectStorage, options?: SqliteStoreOptions) {
    this.storage = storage;
    this.sql = storage.sql;
    this.maxChunkBytes = resolveMaxSqlBytes(
      "maxChunkBytes",
      options?.maxChunkBytes ?? DEFAULT_SQLITE_MAX_CHUNK_BYTES
    );
    this.maxReadBytes = resolveMaxSqlBytes(
      "maxReadBytes",
      options?.maxReadBytes ?? DEFAULT_SQLITE_MAX_READ_BYTES
    );
    this.maxAppendBytes = resolveMaxLogicalBytes(
      "maxAppendBytes",
      options?.maxAppendBytes ?? DEFAULT_SQLITE_MAX_APPEND_BYTES,
      this.maxChunkBytes
    );
  }

  initialize(): void {
    initializeSqliteStreamsSchema(this.sql);
    this.sql.exec(SqliteStore.chunkSchema);
    this.sql.exec(SqliteStore.producerSchema);
    this.sql.exec(SqliteStore.producerAppendSchema);
  }

  /**
   * commits stream metadata and chunk rows together.
   * NOTE: failed chunk writes fail before a stream row claims bytes that were never stored.
   */
  private writeTransaction<T>(operation: () => T): T {
    return this.storage.transactionSync(operation);
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
    const deletedPaths: string[] = [];
    this.writeTransaction(() => {
      this.hardDeleteInTransaction(path, row, deletedPaths);
    });
    for (const deletedPath of deletedPaths) {
      this.notifyDeleted(deletedPath);
    }
  }

  private hardDeleteInTransaction(
    path: string,
    row: StreamRow,
    deletedPaths: string[]
  ): void {
    this.sql.exec(
      "DELETE FROM stream_producers WHERE path = ? AND incarnation = ?",
      path,
      row.incarnation
    );
    this.sql.exec(
      "DELETE FROM stream_producer_appends WHERE path = ? AND incarnation = ?",
      path,
      row.incarnation
    );
    this.sql.exec(
      "DELETE FROM stream_chunks WHERE path = ? AND incarnation = ?",
      path,
      row.incarnation
    );
    this.sql.exec("DELETE FROM streams WHERE path = ?", path);
    deletedPaths.push(path);
    this.releaseParentInTransaction(row.forked_from ?? undefined, deletedPaths);
  }

  private releaseParentInTransaction(
    parentPath: string | undefined,
    deletedPaths: string[]
  ): void {
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
    this.sql.exec(
      "UPDATE streams SET child_count = ? WHERE path = ?",
      childCount,
      parentPath
    );
    if (parent.deleted === 1 && childCount === 0) {
      this.hardDeleteInTransaction(
        parentPath,
        { ...parent, child_count: childCount },
        deletedPaths
      );
    }
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
      this.assertAppendSize(prepared.data.length);
      return {
        ...prepared,
        incarnation: generateIncarnation(),
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

    const forkOffset = options.forkOffset ?? source.next_offset;
    const forkSubOffset = normalizeForkSubOffset(options.forkSubOffset);
    const sourceData = this.readForkSourceBytes(
      sourcePath,
      source.next_offset,
      forkOffset,
      source.content_type,
      forkSubOffset,
      source.incarnation
    );
    let prepared: ReturnType<typeof prepareForkData>;
    try {
      prepared = prepareForkData(
        sourceData,
        forkOffset,
        source.next_offset,
        source.content_type,
        forkSubOffset,
        options.data
      );
    } catch (error) {
      this.rethrowTruncatedForkPayloadTooLarge(
        error,
        source.next_offset,
        forkSubOffset
      );
      throw error;
    }
    this.assertAppendSize(prepared.data.length);
    const { ttlSeconds, expiresAt } = inheritedExpiration(
      {
        ttlSeconds: source.ttl_seconds ?? undefined,
        expiresAt: source.expires_at ?? undefined,
      },
      options
    );

    return {
      ...prepared,
      incarnation: generateIncarnation(),
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
      incarnation: existing.incarnation,
      nextOffset: existing.next_offset,
      contentType: existing.content_type,
      closed: existing.closed === 1,
    };
  }

  put(path: string, options: PutOptions): Promise<PutResult> {
    const existing = this.getStreamRow(path);

    if (existing) {
      assertStreamIncarnation(
        path,
        existing.incarnation,
        options.expectedIncarnation
      );
      return Promise.resolve(this.idempotentCreateResult(existing, options));
    }

    if (options.expectedIncarnation !== undefined) {
      throw new StreamConflictError(`stream incarnation is stale: ${path}`);
    }

    const prepared = this.prepareCreate(options);
    const now = Date.now();
    try {
      this.writeTransaction(() => {
        if (prepared.forkedFrom !== undefined) {
          this.sql.exec(
            "UPDATE streams SET child_count = child_count + 1 WHERE path = ?",
            prepared.forkedFrom
          );
        }

        this.sql.exec(
          `INSERT INTO streams (path, incarnation, content_type, ttl_seconds, expires_at, created_at, last_accessed_at, next_offset, last_seq, producer_id, producer_epoch, next_producer_sequence, append_count, closed, forked_from, fork_offset, fork_sub_offset, child_count, deleted)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
          path,
          prepared.incarnation,
          prepared.contentType,
          prepared.ttlSeconds ?? null,
          prepared.expiresAt ?? null,
          now,
          now,
          prepared.nextOffset,
          null,
          null,
          0,
          0,
          prepared.appendCount,
          prepared.closed ? 1 : 0,
          prepared.forkedFrom ?? null,
          prepared.forkOffset ?? null,
          prepared.forkSubOffset ?? null,
          0,
          0
        );
        this.insertInitialChunks(
          path,
          prepared.incarnation,
          prepared.data,
          prepared.appendCount,
          prepared.nextOffset
        );
      });
    } catch (error) {
      rethrowSqlPayloadTooLargeError(
        error,
        Math.min(prepared.data.length, this.maxChunkBytes)
      );
    }

    this.streamCache.set(path, { contentType: prepared.contentType });
    return Promise.resolve({
      created: true,
      incarnation: prepared.incarnation,
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
    assertStreamIncarnation(
      path,
      stream.incarnation,
      options?.expectedIncarnation
    );

    const producerDecision = this.evaluateProducerDecision(
      stream,
      options?.producer
    );
    if (data.length > 0) {
      validateAppendContentType(stream.content_type, options?.contentType);
    }

    const append = this.prepareAppendChunk(
      data,
      stream.content_type,
      stream.append_count,
      stream.next_offset
    );
    const shouldClose = options?.close === true;
    this.assertMeaningfulAppend(append, shouldClose);

    if (producerDecision._tag === "Duplicate") {
      const retryEndOffset = this.assertProducerRetryMatches(
        path,
        options?.producer,
        append,
        shouldClose,
        stream.incarnation
      );
      this.touchStream(path, stream);
      return Promise.resolve({
        incarnation: stream.incarnation,
        nextOffset: retryEndOffset,
        producer: producerDecision.result,
        closed: stream.closed === 1,
        appended: false,
      });
    }

    if (stream.closed === 1) {
      if (options?.producer !== undefined) {
        let retryEndOffset: Offset;
        try {
          retryEndOffset = this.assertProducerRetryMatches(
            path,
            options.producer,
            append,
            shouldClose,
            stream.incarnation
          );
        } catch (error) {
          if (!(error instanceof StreamConflictError)) {
            throw error;
          }
          throw new StreamClosedError(path, stream.next_offset);
        }
        this.touchStream(path, stream);
        return Promise.resolve({
          incarnation: stream.incarnation,
          nextOffset: retryEndOffset,
          producer: { ...options.producer, duplicate: true },
          closed: true,
          appended: false,
        });
      }
      const closedResult = closedAppendResult(
        path,
        stream.incarnation,
        stream.next_offset,
        true,
        data,
        options,
        producerDecision
      );
      if (closedResult) {
        this.touchStream(path, stream);
        return Promise.resolve(closedResult);
      }
    }
    validateAppendSeq(stream.last_seq ?? undefined, options?.seq);

    try {
      this.writeTransaction(() => {
        const touched = this.touchStream(path, stream);
        const nextProducerSequence =
          stream.producer_id !== null && producerDecision._tag === "Accepted"
            ? stream.next_producer_sequence + 1
            : stream.next_producer_sequence;

        this.insertAppendChunks(
          path,
          stream.incarnation,
          stream.next_offset,
          append
        );
        this.writeProducerState(
          path,
          stream.incarnation,
          producerDecision,
          stream.producer_id !== null
        );
        this.writeProducerAppend(
          path,
          stream.incarnation,
          stream.next_offset,
          append,
          producerDecision,
          shouldClose
        );

        this.sql.exec(
          "UPDATE streams SET next_offset = ?, append_count = ?, last_seq = ?, closed = ?, last_accessed_at = ?, next_producer_sequence = ? WHERE path = ?",
          append.nextOffset,
          append.appendCount,
          options?.seq ?? stream.last_seq,
          shouldClose ? 1 : 0,
          touched.last_accessed_at,
          nextProducerSequence,
          path
        );
      });
    } catch (error) {
      rethrowSqlPayloadTooLargeError(error, append.data.length);
    }

    this.notifyWaitersAfterCommit(
      path,
      append.nextOffset,
      stream.content_type,
      stream.incarnation,
      shouldClose
    );

    return Promise.resolve(
      appendResult(
        stream.incarnation,
        append.nextOffset,
        shouldClose,
        append.appended,
        producerDecision
      )
    );
  }

  acquireProducer(path: string, producerId: string): Promise<ProducerClaim> {
    if (producerId.length === 0) {
      throw new InvalidProducerError("Producer-Id must not be empty");
    }

    const stream = this.getStreamRow(path);
    if (!stream) {
      throw new StreamNotFoundError(path);
    }
    assertStreamLive(path, { deleted: stream.deleted === 1 });

    const epoch = stream.producer_epoch + 1;
    this.writeTransaction(() => {
      this.sql.exec(
        "UPDATE streams SET producer_id = ?, producer_epoch = ?, next_producer_sequence = 0 WHERE path = ?",
        producerId,
        epoch,
        path
      );
    });

    return Promise.resolve({
      id: producerId,
      epoch,
      nextSeq: 0,
      incarnation: stream.incarnation,
      nextOffset: stream.next_offset,
    });
  }

  get(path: string, options?: GetOptions): Promise<GetResult> {
    const stream = this.getStreamRow(path);
    if (!stream) {
      throw new StreamNotFoundError(path);
    }
    assertStreamLive(path, { deleted: stream.deleted === 1 });
    assertStreamIncarnation(
      path,
      stream.incarnation,
      options?.expectedIncarnation
    );
    const readStream =
      options?.renewTtl === false ? stream : this.touchStream(path, stream);

    const startOffset = options?.offset ?? initialOffset();
    const window = this.readWindow(
      path,
      startOffset,
      readStream.next_offset,
      readStream.content_type,
      readStream.incarnation
    );

    return Promise.resolve({
      messages: window.messages,
      incarnation: readStream.incarnation,
      nextOffset: window.nextOffset,
      upToDate: window.upToDate,
      cursor: calculateCursor(),
      etag: generateETag(path, startOffset, window.nextOffset),
      contentType: readStream.content_type,
      closed: readStream.closed === 1 && window.upToDate,
    });
  }

  head(path: string): Promise<HeadResult | null> {
    const stream = this.getStreamRow(path);
    if (!stream) {
      return Promise.resolve(null);
    }
    assertStreamLive(path, { deleted: stream.deleted === 1 });

    return Promise.resolve({
      incarnation: stream.incarnation,
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

  has(path: string): Promise<boolean> {
    const stream = this.getStreamRow(path);
    return Promise.resolve(stream !== null && stream.deleted !== 1);
  }

  waitForData(
    path: string,
    offset: Offset,
    timeoutMs: number,
    options?: WaitOptions
  ): Promise<WaitResult> {
    const stream = this.getStreamRow(path);
    if (!stream) {
      throw new StreamNotFoundError(path);
    }
    assertStreamLive(path, { deleted: stream.deleted === 1 });
    assertStreamIncarnation(
      path,
      stream.incarnation,
      options?.expectedIncarnation
    );
    const readStream =
      options?.renewTtl === false ? stream : this.touchStream(path, stream);

    const window = this.readWindow(
      path,
      offset,
      readStream.next_offset,
      readStream.content_type,
      readStream.incarnation
    );
    if (window.messages.length > 0) {
      return Promise.resolve({
        messages: window.messages,
        timedOut: false,
        incarnation: readStream.incarnation,
        closed: readStream.closed === 1 && window.upToDate,
      });
    }

    if (readStream.closed === 1) {
      return Promise.resolve({
        messages: [],
        timedOut: false,
        incarnation: readStream.incarnation,
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
      timeoutMs,
      { incarnation: readStream.incarnation }
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
    this.assertAppendSize(chunkData.length);

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

  private assertAppendSize(size: number): void {
    if (size > this.maxAppendBytes) {
      throw new PayloadTooLargeError(this.maxAppendBytes, size);
    }
  }

  private assertMeaningfulAppend(
    append: PreparedAppendChunk,
    closed: boolean
  ): void {
    if (!append.appended && !closed) {
      throw new StreamConflictError("empty append must close the stream");
    }
  }

  private getProducerState(
    stream: StreamRow,
    producer: AppendOptions["producer"]
  ): ProducerState | undefined {
    if (producer === undefined) {
      return;
    }

    const rows = this.sql
      .exec(
        "SELECT epoch, seq FROM stream_producers WHERE path = ? AND incarnation = ? AND producer_id = ?",
        stream.path,
        stream.incarnation,
        producer.id
      )
      .toArray() as ProducerRow[];
    if (rows.length > 0) {
      const row = rows[0] as ProducerRow;
      return { epoch: row.epoch, seq: row.seq };
    }

    return;
  }

  private evaluateProducerDecision(
    stream: StreamRow,
    producer: AppendOptions["producer"]
  ): ProducerAppendDecision {
    if (stream.producer_id !== null) {
      return evaluateClaimedProducerAppend(
        {
          id: stream.producer_id,
          epoch: stream.producer_epoch,
          nextSeq: stream.next_producer_sequence,
          incarnation: stream.incarnation,
          nextOffset: stream.next_offset,
        },
        producer
      );
    }

    const producerState = this.getProducerState(stream, producer);
    const producerStates =
      producer === undefined || producerState === undefined
        ? {}
        : { [producer.id]: producerState };
    return evaluateProducerAppend(producerStates, producer);
  }

  private writeProducerState(
    path: string,
    incarnation: StreamIncarnation,
    decision: ProducerAppendDecision,
    claimed: boolean
  ): void {
    if (claimed || decision._tag !== "Accepted") {
      return;
    }

    this.sql.exec(
      `INSERT INTO stream_producers (path, incarnation, producer_id, epoch, seq)
       VALUES (?, ?, ?, ?, ?)
       ON CONFLICT(path, incarnation, producer_id) DO UPDATE SET
         epoch = excluded.epoch,
         seq = excluded.seq`,
      path,
      incarnation,
      decision.result.id,
      decision.nextState.epoch,
      decision.nextState.seq
    );
  }

  private writeProducerAppend(
    path: string,
    incarnation: StreamIncarnation,
    startOffset: Offset,
    append: PreparedAppendChunk,
    decision: ProducerAppendDecision,
    closed: boolean
  ): void {
    if (decision._tag !== "Accepted") {
      return;
    }

    this.sql.exec(
      `INSERT INTO stream_producer_appends (path, incarnation, producer_id, epoch, seq, append_index, start_offset, end_offset, data_length, closed)
       VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
      path,
      incarnation,
      decision.result.id,
      decision.result.epoch,
      decision.result.seq,
      append.appendCount,
      startOffset,
      append.nextOffset,
      append.data.length,
      closed ? 1 : 0
    );
  }

  private assertProducerRetryMatches(
    path: string,
    producer: AppendOptions["producer"],
    append: PreparedAppendChunk,
    closed: boolean,
    incarnation: StreamIncarnation
  ): Offset {
    if (producer === undefined) {
      throw new StreamConflictError(
        `producer retry receipt is missing: ${path}`
      );
    }

    const rows = this.sql
      .exec(
        `SELECT start_offset, end_offset, data_length, closed
         FROM stream_producer_appends
         WHERE path = ? AND incarnation = ? AND producer_id = ? AND epoch = ? AND seq = ?`,
        path,
        incarnation,
        producer.id,
        producer.epoch,
        producer.seq
      )
      .toArray() as ProducerAppendRow[];
    const row = rows[0];
    if (row === undefined) {
      throw new StreamConflictError(
        `producer sequence has no stored append: ${path}`
      );
    }
    const receiptClosed = Number(row.closed) === 1;
    if (receiptClosed && closed) {
      return row.end_offset;
    }
    if (
      receiptClosed !== closed ||
      row.data_length !== append.data.length
    ) {
      throw new StreamConflictError(
        `producer sequence has conflicting content: ${path}`
      );
    }
    if (append.data.length > 0) {
      const stored = this.readAppendBytes(
        path,
        row.start_offset,
        row.end_offset,
        incarnation
      );
      if (!bytesEqual(stored, append.data)) {
        throw new StreamConflictError(
          `producer sequence has conflicting content: ${path}`
        );
      }
    }
    return row.end_offset;
  }

  private insertInitialChunks(
    path: string,
    incarnation: StreamIncarnation,
    data: Uint8Array,
    appendIndex: number,
    finalOffset: Offset
  ): void {
    if (data.length === 0) {
      return;
    }

    const chunkCount = Math.ceil(data.length / this.maxChunkBytes);
    let chunkIndex = 0;
    let startPos = 0;
    while (startPos < data.length) {
      const endPos = Math.min(startPos + this.maxChunkBytes, data.length);
      const chunk = data.slice(startPos, endPos);
      const endOffset =
        endPos === data.length
          ? finalOffset
          : formatOffset(appendIndex, endPos);
      this.insertChunk(
        path,
        incarnation,
        appendIndex,
        chunkIndex,
        chunkCount,
        startPos,
        chunk,
        startPos === 0 ? initialOffset() : formatOffset(appendIndex, startPos),
        endOffset
      );
      chunkIndex += 1;
      startPos = endPos;
    }
  }

  private insertAppendChunks(
    path: string,
    incarnation: StreamIncarnation,
    startOffset: Offset,
    append: PreparedAppendChunk
  ): void {
    if (!append.appended) {
      return;
    }

    const appendIndex = append.appendCount;
    const basePos = offsetToBytePos(startOffset);
    const chunkCount = Math.ceil(append.data.length / this.maxChunkBytes);
    let chunkIndex = 0;
    let offset = 0;
    while (offset < append.data.length) {
      const nextOffset = Math.min(
        offset + this.maxChunkBytes,
        append.data.length
      );
      const chunk = append.data.slice(offset, nextOffset);
      const startPos = basePos + offset;
      const endPos = basePos + nextOffset;
      this.insertChunk(
        path,
        incarnation,
        appendIndex,
        chunkIndex,
        chunkCount,
        startPos,
        chunk,
        offset === 0 ? startOffset : formatOffset(appendIndex, startPos),
        nextOffset === append.data.length
          ? append.nextOffset
          : formatOffset(appendIndex, endPos)
      );
      chunkIndex += 1;
      offset = nextOffset;
    }
  }

  private insertChunk(
    path: string,
    incarnation: StreamIncarnation,
    appendIndex: number,
    chunkIndex: number,
    chunkCount: number,
    startPos: number,
    data: Uint8Array,
    startOffset: Offset,
    endOffset: Offset
  ): void {
    this.assertChunkSize(data.length);
    try {
      this.sql.exec(
        `INSERT INTO stream_chunks (path, incarnation, append_index, chunk_index, chunk_count, start_pos, end_pos, start_offset, end_offset, data)
         VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
        path,
        incarnation,
        appendIndex,
        chunkIndex,
        chunkCount,
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

  private readBytes(
    path: string,
    tailOffset: Offset,
    incarnation: StreamIncarnation
  ): Uint8Array {
    const messages = this.readAllMessages(
      path,
      initialOffset(),
      tailOffset,
      incarnation
    );
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

  private readAppendBytes(
    path: string,
    startOffset: Offset,
    endOffset: Offset,
    incarnation: StreamIncarnation
  ): Uint8Array {
    const messages = this.readMessages(
      path,
      startOffset,
      offsetToBytePos(endOffset),
      true,
      incarnation
    ).messages;
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

  private readForkSourceBytes(
    path: string,
    sourceTailOffset: Offset,
    forkOffset: Offset,
    contentType: string,
    forkSubOffset: number | undefined,
    incarnation: StreamIncarnation
  ): Uint8Array {
    const { byteOffset: forkPos, tailPos: sourceTailPos } = validateReadOffset(
      forkOffset,
      sourceTailOffset
    );
    if (sourceTailPos <= this.maxAppendBytes) {
      return this.readBytes(path, sourceTailOffset, incarnation);
    }
    if (forkPos > this.maxAppendBytes) {
      throw new PayloadTooLargeError(this.maxAppendBytes, forkPos);
    }
    if (!isJsonContentType(contentType) && forkSubOffset !== undefined) {
      const requestedBytes = forkPos + forkSubOffset;
      if (requestedBytes > this.maxAppendBytes) {
        throw new PayloadTooLargeError(this.maxAppendBytes, requestedBytes);
      }
    }
    return this.readPrefixBytes(
      path,
      Math.min(sourceTailPos, this.maxAppendBytes + 1),
      incarnation
    );
  }

  private rethrowTruncatedForkPayloadTooLarge(
    error: unknown,
    sourceTailOffset: Offset,
    forkSubOffset: number | undefined
  ): void {
    if (
      error instanceof InvalidOffsetError &&
      forkSubOffset !== undefined &&
      offsetToBytePos(sourceTailOffset) > this.maxAppendBytes
    ) {
      throw new PayloadTooLargeError(
        this.maxAppendBytes,
        this.maxAppendBytes + 1
      );
    }
  }

  private readPrefixBytes(
    path: string,
    endPos: number,
    incarnation: StreamIncarnation
  ): Uint8Array {
    const messages = this.readMessages(
      path,
      initialOffset(),
      endPos,
      false,
      incarnation
    ).messages;
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

  private readWindow(
    path: string,
    startOffset: Offset,
    tailOffset: Offset,
    contentType: string,
    incarnation: StreamIncarnation
  ): ReadWindow {
    const { byteOffset: startPos, tailPos } = validateReadOffset(
      startOffset,
      tailOffset
    );
    if (startPos === tailPos) {
      return { messages: [], nextOffset: tailOffset, upToDate: true };
    }

    const isJson = isJsonContentType(contentType);
    let windowEndPos = Math.min(startPos + this.maxReadBytes, tailPos);
    if (isJson && windowEndPos < tailPos) {
      windowEndPos = this.expandJsonWindowEndPos(
        path,
        windowEndPos,
        incarnation
      );
    }
    const result = this.readMessages(
      path,
      startOffset,
      windowEndPos,
      isJson,
      incarnation
    );
    if (result.messages.length === 0) {
      throw new StreamConflictError("stream chunk range is incomplete");
    }

    return {
      messages: result.messages,
      nextOffset: windowEndPos === tailPos ? tailOffset : result.nextOffset,
      upToDate: windowEndPos === tailPos,
    };
  }

  /**
   * keeps JSON reads aligned to complete logical appends.
   * NOTE: byte windows may cut binary streams anywhere, but JSON responses never wrap a partial object in `[...]`.
   */
  private expandJsonWindowEndPos(
    path: string,
    proposedEndPos: number,
    incarnation: StreamIncarnation
  ): number {
    const rows = this.sql
      .exec(
        `SELECT append_index, chunk_index, chunk_count
         FROM stream_chunks
         WHERE path = ? AND incarnation = ?
           AND ((start_pos < ? AND end_pos > ?) OR (start_pos = ? AND chunk_index > 0))
         ORDER BY start_pos
         LIMIT 1`,
        path,
        incarnation,
        proposedEndPos,
        proposedEndPos,
        proposedEndPos
      )
      .toArray() as Array<{
        append_index: number;
        chunk_index: number;
        chunk_count: number;
      }>;
    const row = rows[0];
    if (row === undefined) {
      return proposedEndPos;
    }

    const endRows = this.sql
      .exec(
        "SELECT MAX(end_pos) AS end_pos FROM stream_chunks WHERE path = ? AND incarnation = ? AND append_index = ?",
        path,
        incarnation,
        row.append_index
      )
      .toArray() as Array<{ end_pos: number | null }>;
    const endPos = endRows[0]?.end_pos;
    if (
      typeof endPos !== "number" ||
      !Number.isSafeInteger(endPos) ||
      endPos <= proposedEndPos
    ) {
      throw new StreamConflictError(
        `stream chunk range is incomplete: ${path}`
      );
    }
    return endPos;
  }

  private readAllMessages(
    path: string,
    startOffset: Offset,
    tailOffset: Offset,
    incarnation: StreamIncarnation
  ): StreamMessage[] {
    return this.readMessages(
      path,
      startOffset,
      offsetToBytePos(tailOffset),
      true,
      incarnation
    ).messages;
  }

  private readMessages(
    path: string,
    startOffset: Offset,
    endPos?: number,
    requireCompleteGroups = endPos === undefined,
    incarnation?: StreamIncarnation
  ): SqlChunkMessagesResult {
    if (incarnation === undefined) {
      throw new StreamConflictError(`stream incarnation is missing: ${path}`);
    }
    const startPos = offsetToBytePos(startOffset);
    return readSqlChunkMessages(
      path,
      startOffset,
      incarnation,
      this.readChunkRows(path, startPos, incarnation, endPos),
      endPos,
      requireCompleteGroups
    );
  }

  private readChunkRows(
    path: string,
    startPos: number,
    incarnation: StreamIncarnation,
    endPos?: number
  ): ChunkRow[] {
    if (endPos === undefined) {
      return this.sql
        .exec(
          `SELECT append_index, incarnation, chunk_index, chunk_count, start_pos, end_pos, start_offset, end_offset, data
           FROM stream_chunks
           WHERE path = ? AND incarnation = ? AND end_pos > ?
           ORDER BY start_pos`,
          path,
          incarnation,
          startPos
        )
        .toArray() as ChunkRow[];
    }

    return this.sql
      .exec(
        `SELECT append_index, incarnation, chunk_index, chunk_count, start_pos, end_pos, start_offset, end_offset, data
         FROM stream_chunks
         WHERE path = ? AND incarnation = ? AND end_pos > ? AND start_pos < ?
         ORDER BY start_pos`,
        path,
        incarnation,
        startPos,
        endPos
      )
      .toArray() as ChunkRow[];
  }

  /**
   * wakes parked readers after the append transaction commits.
   * NOTE: waiter delivery reads must not make an already committed append look like it failed.
   */
  private notifyWaitersAfterCommit(
    path: string,
    tailOffset: Offset,
    contentType: string,
    incarnation: StreamIncarnation,
    closed = false
  ): void {
    try {
      this.notifyWaiters(path, tailOffset, contentType, incarnation, closed);
    } catch {
      const waiters = this.waiters.get(path) ?? [];
      this.waiters.delete(path);
      Effect.runSync(
        Effect.forEach(waiters, (waiter) =>
          Deferred.succeed(waiter.deferred, {
            messages: [],
            timedOut: false,
            incarnation,
          })
        )
      );
    }
  }

  private notifyWaiters(
    path: string,
    tailOffset: Offset,
    contentType: string,
    incarnation: StreamIncarnation,
    closed = false
  ): void {
    const waiters = this.waiters.get(path) ?? [];
    this.waiters.set(path, []);

    for (const waiter of waiters) {
      if (
        waiter.incarnation !== undefined &&
        waiter.incarnation !== incarnation
      ) {
        Effect.runSync(
          Deferred.succeed(waiter.deferred, {
            messages: [],
            timedOut: false,
            incarnation,
          })
        );
        continue;
      }

      let window: ReadWindow;
      try {
        window = this.readWindow(
          path,
          waiter.offset,
          tailOffset,
          contentType,
          incarnation
        );
      } catch {
        Effect.runSync(
          Deferred.succeed(waiter.deferred, {
            messages: [],
            timedOut: false,
            incarnation,
          })
        );
        continue;
      }
      const closedInWindow = closed && window.upToDate;

      if (window.messages.length > 0 || closedInWindow) {
        Effect.runSync(
          Deferred.succeed(waiter.deferred, {
            messages: window.messages,
            timedOut: false,
            incarnation: waiter.incarnation,
            closed: closedInWindow,
          })
        );
        continue;
      }

      const remaining = this.waiters.get(path) ?? [];
      remaining.push(waiter);
      this.waiters.set(path, remaining);
    }
  }

  private notifyDeleted(path: string): void {
    const waiters = this.waiters.get(path) ?? [];
    notifyDeletedWaiters(waiters);

    this.waiters.delete(path);
    this.streamCache.delete(path);
  }
}
