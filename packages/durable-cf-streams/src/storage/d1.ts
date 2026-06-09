import { Deferred, Effect } from "effect";
import { calculateCursor } from "../cursor.js";
import { StreamNotFoundError } from "../errors.js";
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
  closedAppendResult,
  prepareAppendData,
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
  next_offset: Offset;
  last_seq: string | null;
  producers: string;
  append_count: number;
  closed: number;
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

export class D1Store implements StreamStore {
  private readonly db: D1Database;
  private readonly waiters = new Map<string, Waiter[]>();
  private readonly streamCache = new Map<string, { contentType: string }>();

  constructor(db: D1Database) {
    this.db = db;
  }

  static schema =
    "CREATE TABLE IF NOT EXISTS streams (path TEXT PRIMARY KEY, content_type TEXT NOT NULL, ttl_seconds INTEGER, expires_at TEXT, created_at INTEGER NOT NULL, data BLOB NOT NULL DEFAULT x'', next_offset TEXT NOT NULL, last_seq TEXT, producers TEXT NOT NULL DEFAULT '{}', append_count INTEGER NOT NULL DEFAULT 0, closed INTEGER NOT NULL DEFAULT 0);";

  async initialize(): Promise<void> {
    await this.db.exec(D1Store.schema);
    const columns = await this.db.prepare("PRAGMA table_info(streams)").all<{
      name: string;
    }>();
    if (!columns.results.some((column) => column.name === "closed")) {
      await this.db.exec(
        "ALTER TABLE streams ADD COLUMN closed INTEGER NOT NULL DEFAULT 0"
      );
    }
  }

  async put(path: string, options: PutOptions): Promise<PutResult> {
    const existing = await this.db
      .prepare(
        "SELECT content_type, ttl_seconds, expires_at, created_at, next_offset, closed FROM streams WHERE path = ?"
      )
      .bind(path)
      .first<
        Pick<
          StreamRow,
          | "content_type"
          | "ttl_seconds"
          | "expires_at"
          | "created_at"
          | "next_offset"
          | "closed"
        >
      >();

    if (existing && !isRowExpired(existing)) {
      validateIdempotentCreate(
        {
          contentType: existing.content_type,
          ttlSeconds: existing.ttl_seconds ?? undefined,
          expiresAt: existing.expires_at ?? undefined,
          closed: existing.closed === 1,
        },
        options
      );
      return {
        created: false,
        nextOffset: existing.next_offset,
        closed: existing.closed === 1,
      };
    }

    if (existing) {
      await this.db
        .prepare("DELETE FROM streams WHERE path = ?")
        .bind(path)
        .run();
    }

    const { data, appendCount, nextOffset } = prepareInitialData(options);

    await this.db
      .prepare(`
        INSERT INTO streams (path, content_type, ttl_seconds, expires_at, created_at, data, next_offset, producers, append_count, closed)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
      `)
      .bind(
        path,
        options.contentType,
        options.ttlSeconds ?? null,
        options.expiresAt ?? null,
        Date.now(),
        data,
        nextOffset,
        "{}",
        appendCount,
        options.closed === true ? 1 : 0
      )
      .run();

    this.streamCache.set(path, { contentType: options.contentType });

    return { created: true, nextOffset, closed: options.closed };
  }

  async append(
    path: string,
    data: Uint8Array,
    options?: AppendOptions
  ): Promise<AppendResult> {
    const stream = await this.db
      .prepare(
        "SELECT content_type, ttl_seconds, expires_at, created_at, data, next_offset, last_seq, producers, append_count, closed FROM streams WHERE path = ?"
      )
      .bind(path)
      .first<
        Pick<
          StreamRow,
          | "content_type"
          | "ttl_seconds"
          | "expires_at"
          | "created_at"
          | "data"
          | "next_offset"
          | "last_seq"
          | "producers"
          | "append_count"
          | "closed"
        >
      >();

    if (!stream || isRowExpired(stream)) {
      throw new StreamNotFoundError(path);
    }

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
      return closedResult;
    }

    if (data.length > 0) {
      validateAppendContentType(stream.content_type, options?.contentType);
    }

    if (producerDecision._tag === "Duplicate") {
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

    await this.db
      .prepare(`
        UPDATE streams
        SET data = ?, next_offset = ?, last_seq = ?, producers = ?, append_count = ?, closed = ?
        WHERE path = ?
      `)
      .bind(
        append.data,
        append.nextOffset,
        options?.seq ?? stream.last_seq,
        JSON.stringify(commitProducerAppend(producers, producerDecision)),
        append.appendCount,
        options?.close === true ? 1 : 0,
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
    const stream = await this.db
      .prepare(
        "SELECT content_type, ttl_seconds, expires_at, created_at, data, next_offset, closed FROM streams WHERE path = ?"
      )
      .bind(path)
      .first<
        Pick<
          StreamRow,
          | "content_type"
          | "ttl_seconds"
          | "expires_at"
          | "created_at"
          | "data"
          | "next_offset"
          | "closed"
        >
      >();

    if (!stream || isRowExpired(stream)) {
      this.streamCache.delete(path);
      throw new StreamNotFoundError(path);
    }

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
    const stream = await this.db
      .prepare(
        "SELECT content_type, ttl_seconds, expires_at, created_at, next_offset, closed FROM streams WHERE path = ?"
      )
      .bind(path)
      .first<
        Pick<
          StreamRow,
          | "content_type"
          | "ttl_seconds"
          | "expires_at"
          | "created_at"
          | "next_offset"
          | "closed"
        >
      >();

    if (!stream || isRowExpired(stream)) {
      this.streamCache.delete(path);
      return null;
    }

    this.streamCache.set(path, { contentType: stream.content_type });

    return {
      contentType: stream.content_type,
      nextOffset: stream.next_offset,
      etag: generateETag(path, initialOffset(), stream.next_offset),
      closed: stream.closed === 1,
    };
  }

  async delete(path: string): Promise<void> {
    const waiters = this.waiters.get(path) ?? [];
    const effect = Effect.forEach(waiters, (waiter) =>
      Deferred.succeed(waiter.deferred, { messages: [], timedOut: false })
    );
    Effect.runSync(effect);

    this.waiters.delete(path);
    this.streamCache.delete(path);

    await this.db
      .prepare("DELETE FROM streams WHERE path = ?")
      .bind(path)
      .run();
  }

  has(path: string): boolean {
    return this.streamCache.has(path);
  }

  async waitForData(
    path: string,
    offset: Offset,
    timeoutMs: number
  ): Promise<WaitResult> {
    const stream = await this.db
      .prepare(
        "SELECT ttl_seconds, expires_at, created_at, data, closed FROM streams WHERE path = ?"
      )
      .bind(path)
      .first<
        Pick<
          StreamRow,
          "ttl_seconds" | "expires_at" | "created_at" | "data" | "closed"
        >
      >();

    if (!stream || isRowExpired(stream)) {
      throw new StreamNotFoundError(path);
    }

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
}
