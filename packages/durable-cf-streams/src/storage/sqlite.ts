import { Deferred, Effect } from "effect";
import { calculateCursor } from "../cursor.js";
import { StreamNotFoundError } from "../errors.js";
import { formatOffset, initialOffset, offsetToBytePos } from "../offsets.js";
import {
  formatJsonResponse,
  generateETag,
  isExpired,
  isJsonContentType,
} from "../protocol.js";
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
  mergeData,
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
  data: ArrayBuffer;
  next_offset: string;
  last_seq: string | null;
  append_count: number;
};

type Waiter = {
  deferred: Deferred.Deferred<WaitResult>;
  offset: Offset;
};

const isRowExpired = (row: {
  ttl_seconds: number | null;
  expires_at: string | null;
  created_at: number;
}): boolean =>
  isExpired({
    ttlSeconds: row.ttl_seconds ?? undefined,
    expiresAt: row.expires_at ?? undefined,
    createdAt: row.created_at,
  });

export class SqliteStore implements StreamStore {
  private readonly sql: SqlStorage;
  private readonly waiters = new Map<string, Waiter[]>();
  private readonly streamCache = new Map<string, { contentType: string }>();

  static schema = `
    CREATE TABLE IF NOT EXISTS streams (
      path TEXT PRIMARY KEY,
      content_type TEXT NOT NULL,
      ttl_seconds INTEGER,
      expires_at TEXT,
      created_at INTEGER NOT NULL,
      data BLOB NOT NULL DEFAULT x'',
      next_offset TEXT NOT NULL,
      last_seq TEXT,
      append_count INTEGER NOT NULL DEFAULT 0
    )
  `;

  constructor(sql: SqlStorage) {
    this.sql = sql;
  }

  initialize(): void {
    this.sql.exec(SqliteStore.schema);
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
      this.sql.exec("DELETE FROM streams WHERE path = ?", path);
      this.streamCache.delete(path);
      return null;
    }

    this.streamCache.set(path, { contentType: row.content_type });
    return row;
  }

  put(path: string, options: PutOptions): Promise<PutResult> {
    const existing = this.getStreamRow(path);

    if (existing) {
      validateIdempotentCreate(
        {
          contentType: existing.content_type,
          ttlSeconds: existing.ttl_seconds ?? undefined,
          expiresAt: existing.expires_at ?? undefined,
        },
        options
      );
      return Promise.resolve({
        created: false,
        nextOffset: existing.next_offset,
      });
    }

    const { data, appendCount, nextOffset } = prepareInitialData(options);

    this.sql.exec(
      `INSERT INTO streams (path, content_type, ttl_seconds, expires_at, created_at, data, next_offset, append_count)
       VALUES (?, ?, ?, ?, ?, ?, ?, ?)`,
      path,
      options.contentType,
      options.ttlSeconds ?? null,
      options.expiresAt ?? null,
      Date.now(),
      data,
      nextOffset,
      appendCount
    );

    this.streamCache.set(path, { contentType: options.contentType });
    return Promise.resolve({ created: true, nextOffset });
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

    validateAppendContentType(stream.content_type, options?.contentType);
    validateAppendSeq(stream.last_seq ?? undefined, options?.seq);

    const isJson = isJsonContentType(stream.content_type);
    const existingData = new Uint8Array(stream.data);
    const newData = mergeData(existingData, data, isJson);

    const newAppendCount = stream.append_count + 1;
    const nextOffset = formatOffset(newAppendCount, newData.length);

    this.sql.exec(
      "UPDATE streams SET data = ?, next_offset = ?, append_count = ?, last_seq = ? WHERE path = ?",
      newData,
      nextOffset,
      newAppendCount,
      options?.seq ?? stream.last_seq,
      path
    );

    this.notifyWaiters(path, newData);

    return Promise.resolve({ nextOffset });
  }

  get(path: string, options?: GetOptions): Promise<GetResult> {
    const stream = this.getStreamRow(path);
    if (!stream) {
      throw new StreamNotFoundError(path);
    }

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

    return Promise.resolve({
      messages,
      nextOffset: stream.next_offset,
      upToDate: true,
      cursor: calculateCursor(),
      etag: generateETag(path, startOffset, stream.next_offset),
      contentType: stream.content_type,
    });
  }

  head(path: string): Promise<HeadResult | null> {
    const stream = this.getStreamRow(path);
    if (!stream) {
      return Promise.resolve(null);
    }

    return Promise.resolve({
      contentType: stream.content_type,
      nextOffset: stream.next_offset,
      etag: generateETag(path, initialOffset(), stream.next_offset),
    });
  }

  delete(path: string): Promise<void> {
    const waiters = this.waiters.get(path) ?? [];
    const effect = Effect.forEach(waiters, (waiter) =>
      Deferred.succeed(waiter.deferred, { messages: [], timedOut: false })
    );
    Effect.runSync(effect);

    this.waiters.delete(path);
    this.streamCache.delete(path);
    this.sql.exec("DELETE FROM streams WHERE path = ?", path);
    return Promise.resolve();
  }

  has(path: string): boolean {
    return this.streamCache.has(path) || this.getStreamRow(path) !== null;
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

    const data = new Uint8Array(stream.data);
    const byteOffset = offsetToBytePos(offset);

    if (byteOffset < data.length) {
      return Promise.resolve({
        messages: [
          { offset, timestamp: Date.now(), data: data.slice(byteOffset) },
        ],
        timedOut: false,
      });
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
        this.waiters.set(path, currentWaiters);
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

  private notifyWaiters(path: string, data: Uint8Array): void {
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
        });
      }

      const remaining = this.waiters.get(path) ?? [];
      remaining.push(waiter);
      this.waiters.set(path, remaining);
      return Effect.void;
    });

    Effect.runSync(effect);
  }
}
