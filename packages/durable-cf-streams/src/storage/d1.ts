import { Deferred, Effect } from "effect";
import { calculateCursor } from "../cursor.js";
import { StreamConflictError, StreamNotFoundError } from "../errors.js";
import { initialOffset, offsetToBytePos } from "../offsets.js";
import { commitProducerAppend, evaluateProducerAppend } from "../producer.js";
import {
  formatJsonResponse,
  generateETag,
  isExpired,
  isJsonContentType,
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
  appendResult,
  assertStreamLive,
  closedAppendResult,
  inheritedExpiration,
  prepareAppendData,
  prepareForkData,
  prepareInitialData,
  validateAppendContentType,
  validateAppendSeq,
  validateIdempotentCreate,
} from "./utils.js";

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
  child_count: number;
  deleted: number;
};

type Waiter = {
  deferred: Deferred.Deferred<WaitResult>;
  offset: Offset;
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
};

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

export class D1Store implements StreamStore {
  private readonly db: D1Database;
  private readonly waiters = new Map<string, Waiter[]>();
  private readonly streamCache = new Map<string, { contentType: string }>();

  constructor(db: D1Database) {
    this.db = db;
  }

  static schema =
    "CREATE TABLE IF NOT EXISTS streams (path TEXT PRIMARY KEY, content_type TEXT NOT NULL, ttl_seconds INTEGER, expires_at TEXT, created_at INTEGER NOT NULL, last_accessed_at INTEGER, data BLOB NOT NULL DEFAULT x'', next_offset TEXT NOT NULL, last_seq TEXT, producers TEXT NOT NULL DEFAULT '{}', append_count INTEGER NOT NULL DEFAULT 0, closed INTEGER NOT NULL DEFAULT 0, forked_from TEXT, fork_offset TEXT, child_count INTEGER NOT NULL DEFAULT 0, deleted INTEGER NOT NULL DEFAULT 0);";

  async initialize(): Promise<void> {
    await this.db.exec(D1Store.schema);
    const columns = await this.db.prepare("PRAGMA table_info(streams)").all<{
      name: string;
    }>();
    const hasColumn = (name: string) =>
      columns.results.some((column) => column.name === name);
    const addColumn = async (name: string, sql: string) => {
      if (!hasColumn(name)) {
        await this.db.exec(sql);
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
      "child_count",
      "ALTER TABLE streams ADD COLUMN child_count INTEGER NOT NULL DEFAULT 0"
    );
    await addColumn(
      "deleted",
      "ALTER TABLE streams ADD COLUMN deleted INTEGER NOT NULL DEFAULT 0"
    );
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
    await this.db
      .prepare("DELETE FROM streams WHERE path = ?")
      .bind(path)
      .run();
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

  private async prepareCreate(options: PutOptions): Promise<PreparedCreate> {
    if (options.forkedFrom === undefined) {
      const prepared = prepareInitialData(options);
      return {
        ...prepared,
        contentType: options.contentType,
        ttlSeconds: options.ttlSeconds,
        expiresAt: options.expiresAt,
        closed: options.closed === true,
      };
    }

    return await this.prepareForkCreate(options, options.forkedFrom);
  }

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

    const forkOffset = options.forkOffset ?? source.next_offset;
    const prepared = prepareForkData(new Uint8Array(source.data), forkOffset);
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
      },
      options
    );

    return {
      created: false,
      nextOffset: existing.next_offset,
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
    await this.db
      .prepare(`
        INSERT INTO streams (path, content_type, ttl_seconds, expires_at, created_at, last_accessed_at, data, next_offset, producers, append_count, closed, forked_from, fork_offset, child_count, deleted)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
      `)
      .bind(
        path,
        prepared.contentType,
        prepared.ttlSeconds ?? null,
        prepared.expiresAt ?? null,
        now,
        now,
        prepared.data,
        prepared.nextOffset,
        "{}",
        prepared.appendCount,
        prepared.closed ? 1 : 0,
        prepared.forkedFrom ?? null,
        prepared.forkOffset ?? null,
        0,
        0
      )
      .run();

    this.streamCache.set(path, { contentType: prepared.contentType });

    return {
      created: true,
      nextOffset: prepared.nextOffset,
      closed: prepared.closed,
    };
  }

  async append(
    path: string,
    data: Uint8Array,
    options?: AppendOptions
  ): Promise<AppendResult> {
    let stream = await this.getStreamRow(path);

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

    const existingData = new Uint8Array(stream.data);
    const append = prepareAppendData(
      existingData,
      data,
      stream.content_type,
      stream.append_count,
      stream.next_offset
    );
    stream = await this.touchStream(path, stream);

    await this.db
      .prepare(`
        UPDATE streams
        SET data = ?, next_offset = ?, last_seq = ?, producers = ?, append_count = ?, closed = ?, last_accessed_at = ?
        WHERE path = ?
      `)
      .bind(
        append.data,
        append.nextOffset,
        options?.seq ?? stream.last_seq,
        JSON.stringify(commitProducerAppend(producers, producerDecision)),
        append.appendCount,
        options?.close === true ? 1 : 0,
        stream.last_accessed_at,
        path
      )
      .run();

    this.notifyWaiters(path, append.data, options?.close === true);

    return appendResult(
      append.nextOffset,
      options?.close === true,
      append.appended,
      producerDecision
    );
  }

  async get(path: string, options?: GetOptions): Promise<GetResult> {
    let stream = await this.getStreamRow(path);

    if (!stream) {
      this.streamCache.delete(path);
      throw new StreamNotFoundError(path);
    }
    assertStreamLive(path, { deleted: stream.deleted === 1 });
    stream = await this.touchStream(path, stream);

    this.streamCache.set(path, { contentType: stream.content_type });

    const startOffset = options?.offset ?? initialOffset();
    const byteOffset = offsetToBytePos(startOffset);

    const data = new Uint8Array(stream.data);
    const messages: StreamMessage[] = [];

    if (byteOffset < data.length) {
      messages.push({
        offset: startOffset,
        timestamp: Date.now(),
        data: data.slice(byteOffset),
      });
    }

    return {
      messages,
      nextOffset: stream.next_offset,
      upToDate: true,
      cursor: calculateCursor(),
      etag: generateETag(path, startOffset, stream.next_offset),
      contentType: stream.content_type,
      closed: stream.closed === 1,
    };
  }

  async head(path: string): Promise<HeadResult | null> {
    const stream = await this.getStreamRow(path);

    if (!stream) {
      this.streamCache.delete(path);
      return null;
    }
    assertStreamLive(path, { deleted: stream.deleted === 1 });

    this.streamCache.set(path, { contentType: stream.content_type });

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
    let stream = await this.getStreamRow(path);

    if (!stream) {
      throw new StreamNotFoundError(path);
    }
    assertStreamLive(path, { deleted: stream.deleted === 1 });
    stream = await this.touchStream(path, stream);

    const data = new Uint8Array(stream.data);
    const byteOffset = offsetToBytePos(offset);

    if (byteOffset < data.length) {
      return {
        messages: [
          { offset, timestamp: Date.now(), data: data.slice(byteOffset) },
        ],
        timedOut: false,
        closed: stream.closed === 1,
      };
    }

    if (stream.closed === 1) {
      return { messages: [], timedOut: false, closed: true };
    }

    const effect = Effect.gen(this, function* () {
      const deferred = yield* Deferred.make<WaitResult>();
      const waiter: Waiter = { deferred, offset };

      const pathWaiters = this.waiters.get(path) ?? [];
      pathWaiters.push(waiter);
      this.waiters.set(path, pathWaiters);

      const timeout = Effect.as(
        Effect.delay(
          Effect.sync(() => {
            // no op
          }),
          timeoutMs
        ),
        { messages: [], timedOut: true } as WaitResult
      );

      const result = yield* Effect.race(Deferred.await(deferred), timeout);

      const currentWaiters = this.waiters.get(path) ?? [];
      const index = currentWaiters.indexOf(waiter);
      if (index !== -1) {
        currentWaiters.splice(index, 1);
      }

      return result;
    });

    return Effect.runPromise(effect);
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

  private notifyWaiters(path: string, data: Uint8Array, closed = false): void {
    const waiters = this.waiters.get(path) ?? [];
    this.waiters.set(path, []);

    const effect = Effect.forEach(waiters, (waiter) => {
      const byteOffset = offsetToBytePos(waiter.offset);

      if (byteOffset < data.length) {
        return Deferred.succeed(waiter.deferred, {
          messages: [
            {
              offset: waiter.offset,
              timestamp: Date.now(),
              data: data.slice(byteOffset),
            },
          ],
          timedOut: false,
          closed,
        });
      }
      if (closed) {
        return Deferred.succeed(waiter.deferred, {
          messages: [],
          timedOut: false,
          closed: true,
        });
      }
      const remaining = this.waiters.get(path) ?? [];
      remaining.push(waiter);
      this.waiters.set(path, remaining);
      return Effect.void;
    });

    Effect.runSync(effect);
  }

  private notifyDeleted(path: string): void {
    const waiters = this.waiters.get(path) ?? [];
    const effect = Effect.forEach(waiters, (waiter) =>
      Deferred.succeed(waiter.deferred, { messages: [], timedOut: false })
    );
    Effect.runSync(effect);

    this.waiters.delete(path);
    this.streamCache.delete(path);
  }
}
