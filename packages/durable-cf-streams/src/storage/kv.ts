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
import {
  decodePersistedStreamMetadata,
  type PersistedStreamMetadata,
} from "../schema.js";
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

type StreamRecord = PersistedStreamMetadata;

type Waiter = {
  deferred: Deferred.Deferred<WaitResult>;
  offset: Offset;
};

export class KVStore implements StreamStore {
  private readonly kv: KVNamespace;
  private readonly waiters = new Map<string, Waiter[]>();
  private readonly streamCache = new Map<string, { contentType: string }>();

  constructor(kv: KVNamespace) {
    this.kv = kv;
  }

  private metaKey(path: string): string {
    return `stream:${path}:meta`;
  }

  private dataKey(path: string): string {
    return `stream:${path}:data`;
  }

  private async getMetadata(path: string): Promise<StreamRecord | null> {
    const metadata = await this.kv.get(this.metaKey(path), "json");
    return metadata === null ? null : decodePersistedStreamMetadata(metadata);
  }

  private async putMetadata(path: string, meta: StreamRecord): Promise<void> {
    await this.kv.put(this.metaKey(path), JSON.stringify(meta));
  }

  private async getData(path: string): Promise<Uint8Array> {
    const raw = await this.kv.get(this.dataKey(path), "arrayBuffer");
    return raw ? new Uint8Array(raw) : new Uint8Array(0);
  }

  private async getStreamMetadata(path: string): Promise<StreamRecord | null> {
    const meta = await this.getMetadata(path);
    if (!meta) {
      return null;
    }

    if (isExpired(meta)) {
      return await this.expireStream(path, meta);
    }

    return meta;
  }

  private async touchMetadata(
    path: string,
    meta: StreamRecord
  ): Promise<StreamRecord> {
    if (meta.ttlSeconds === undefined) {
      return meta;
    }
    const updated = { ...meta, lastAccessedAt: Date.now() };
    await this.putMetadata(path, updated);
    return updated;
  }

  private async expireStream(
    path: string,
    meta: StreamRecord
  ): Promise<StreamRecord | null> {
    if ((meta.childCount ?? 0) > 0) {
      const updated = { ...meta, deleted: true };
      await this.putMetadata(path, updated);
      this.notifyDeleted(path);
      return updated;
    }

    await this.hardDelete(path, meta);
    return null;
  }

  private async hardDelete(path: string, meta: StreamRecord): Promise<void> {
    this.notifyDeleted(path);
    await Promise.all([
      this.kv.delete(this.metaKey(path)),
      this.kv.delete(this.dataKey(path)),
    ]);
    await this.releaseParent(meta.forkedFrom);
  }

  private async releaseParent(parentPath: string | undefined): Promise<void> {
    if (!parentPath) {
      return;
    }

    const parent = await this.getMetadata(parentPath);
    if (!parent) {
      return;
    }

    const childCount = Math.max(0, (parent.childCount ?? 0) - 1);
    const updated = { ...parent, childCount };

    if (updated.deleted === true && childCount === 0) {
      await this.hardDelete(parentPath, updated);
      return;
    }

    await this.putMetadata(parentPath, updated);
  }

  async put(path: string, options: PutOptions): Promise<PutResult> {
    const existingMeta = await this.getStreamMetadata(path);

    if (existingMeta) {
      if (existingMeta.deleted === true) {
        throw new StreamConflictError("stream is gone");
      }
      validateIdempotentCreate(existingMeta, options);
      return {
        created: false,
        nextOffset: existingMeta.nextOffset,
        closed: existingMeta.closed,
      };
    }

    let contentType = options.contentType;
    let ttlSeconds = options.ttlSeconds;
    let expiresAt = options.expiresAt;
    let closed = options.closed === true;
    let forkedFrom: string | undefined;
    let forkOffset: Offset | undefined;
    let prepared = prepareInitialData(options);

    if (options.forkedFrom !== undefined) {
      const source = await this.getStreamMetadata(options.forkedFrom);
      if (!source) {
        throw new StreamNotFoundError(options.forkedFrom);
      }
      if (source.deleted === true) {
        throw new StreamConflictError("fork source is gone");
      }
      validateAppendContentType(source.contentType, contentType);

      const sourceData = await this.getData(options.forkedFrom);
      forkedFrom = options.forkedFrom;
      forkOffset = options.forkOffset ?? source.nextOffset;
      prepared = prepareForkData(sourceData, forkOffset);
      ({ ttlSeconds, expiresAt } = inheritedExpiration(source, options));
      contentType = source.contentType;
      closed = false;

      await this.putMetadata(options.forkedFrom, {
        ...source,
        childCount: (source.childCount ?? 0) + 1,
      });
    }

    const now = Date.now();
    const meta: StreamRecord = {
      contentType,
      ttlSeconds,
      expiresAt,
      createdAt: now,
      lastAccessedAt: now,
      nextOffset: prepared.nextOffset,
      appendCount: prepared.appendCount,
      producers: {},
      closed,
      forkedFrom,
      forkOffset,
      childCount: 0,
      deleted: false,
    };

    await Promise.all([
      this.putMetadata(path, meta),
      this.kv.put(this.dataKey(path), prepared.data),
    ]);

    this.streamCache.set(path, { contentType });

    return { created: true, nextOffset: meta.nextOffset, closed: meta.closed };
  }

  async append(
    path: string,
    data: Uint8Array,
    options?: AppendOptions
  ): Promise<AppendResult> {
    let meta = await this.getStreamMetadata(path);

    if (!meta) {
      throw new StreamNotFoundError(path);
    }
    assertStreamLive(path, meta);

    const producers = meta.producers;
    const producerDecision = evaluateProducerAppend(
      producers,
      options?.producer
    );
    const closedResult = closedAppendResult(
      path,
      meta.nextOffset,
      meta.closed === true,
      data,
      options,
      producerDecision
    );
    if (closedResult) {
      await this.touchMetadata(path, meta);
      return closedResult;
    }

    if (data.length > 0) {
      validateAppendContentType(meta.contentType, options?.contentType);
    }

    if (producerDecision._tag === "Duplicate") {
      await this.touchMetadata(path, meta);
      return {
        nextOffset: meta.nextOffset,
        producer: producerDecision.result,
        closed: meta.closed,
        appended: false,
      };
    }
    validateAppendSeq(meta.lastSeq, options?.seq);

    const existingData = await this.getData(path);

    const append = prepareAppendData(
      existingData,
      data,
      meta.contentType,
      meta.appendCount,
      meta.nextOffset
    );

    meta = await this.touchMetadata(path, meta);
    const updatedMeta: StreamRecord = {
      ...meta,
      nextOffset: append.nextOffset,
      lastSeq: options?.seq ?? meta.lastSeq,
      appendCount: append.appendCount,
      producers: commitProducerAppend(producers, producerDecision),
      closed: options?.close === true,
    };

    await Promise.all([
      this.kv.put(this.metaKey(path), JSON.stringify(updatedMeta)),
      this.kv.put(this.dataKey(path), append.data),
    ]);

    this.notifyWaiters(path, append.data, updatedMeta.closed === true);

    return appendResult(
      updatedMeta.nextOffset,
      updatedMeta.closed === true,
      append.appended,
      producerDecision
    );
  }

  async get(path: string, options?: GetOptions): Promise<GetResult> {
    let meta = await this.getStreamMetadata(path);

    if (!meta) {
      this.streamCache.delete(path);
      throw new StreamNotFoundError(path);
    }
    assertStreamLive(path, meta);
    meta = await this.touchMetadata(path, meta);

    this.streamCache.set(path, { contentType: meta.contentType });

    const data = await this.getData(path);

    const startOffset = options?.offset ?? initialOffset();
    const byteOffset = offsetToBytePos(startOffset);

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
      nextOffset: meta.nextOffset,
      upToDate: true,
      cursor: calculateCursor(),
      etag: generateETag(path, startOffset, meta.nextOffset),
      contentType: meta.contentType,
      closed: meta.closed === true,
    };
  }

  async head(path: string): Promise<HeadResult | null> {
    const meta = await this.getStreamMetadata(path);

    if (!meta) {
      this.streamCache.delete(path);
      return null;
    }
    assertStreamLive(path, meta);

    this.streamCache.set(path, { contentType: meta.contentType });

    return {
      contentType: meta.contentType,
      nextOffset: meta.nextOffset,
      etag: generateETag(path, initialOffset(), meta.nextOffset),
      closed: meta.closed === true,
      ttlSeconds: meta.ttlSeconds,
      expiresAt: meta.expiresAt,
    };
  }

  async delete(path: string): Promise<void> {
    const meta = await this.getStreamMetadata(path);
    if (!meta) {
      return;
    }

    assertStreamLive(path, meta);

    if ((meta.childCount ?? 0) > 0) {
      await this.putMetadata(path, { ...meta, deleted: true });
      this.notifyDeleted(path);
      return;
    }

    await this.hardDelete(path, meta);
  }

  has(path: string): boolean {
    return this.streamCache.has(path);
  }

  async waitForData(
    path: string,
    offset: Offset,
    timeoutMs: number
  ): Promise<WaitResult> {
    let meta = await this.getStreamMetadata(path);

    if (!meta) {
      throw new StreamNotFoundError(path);
    }
    assertStreamLive(path, meta);
    meta = await this.touchMetadata(path, meta);

    const data = await this.getData(path);

    const byteOffset = offsetToBytePos(offset);

    if (byteOffset < data.length) {
      return {
        messages: [
          { offset, timestamp: Date.now(), data: data.slice(byteOffset) },
        ],
        timedOut: false,
        closed: meta.closed,
      };
    }

    if (meta.closed === true) {
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
    let pos = 0;
    for (const message of messages) {
      combined.set(message.data, pos);
      pos += message.data.length;
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
