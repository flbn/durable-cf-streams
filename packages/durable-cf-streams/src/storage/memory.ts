import { Deferred, Effect } from "effect";
import { calculateCursor } from "../cursor.js";
import { StreamConflictError, StreamNotFoundError } from "../errors.js";
import { initialOffset, offsetToBytePos } from "../offsets.js";
import { commitProducerAppend, evaluateProducerAppend } from "../producer.js";
import {
  formatJsonResponse,
  generateETag,
  isJsonContentType,
  isMetadataExpired,
} from "../protocol.js";
import type {
  AppendOptions,
  AppendResult,
  GetOptions,
  GetResult,
  HeadResult,
  Offset,
  ProducerStateMap,
  PutOptions,
  PutResult,
  StreamMessage,
  StreamMetadata,
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

type Waiter = {
  deferred: Deferred.Deferred<WaitResult>;
  offset: Offset;
};

type StoredStream = {
  metadata: StreamMetadata;
  data: Uint8Array;
  nextOffset: Offset;
  lastSeq: string | undefined;
  producers: ProducerStateMap;
  appendCount: number;
  closed: boolean;
  waiters: Waiter[];
};

export class MemoryStore implements StreamStore {
  private readonly streams = new Map<string, StoredStream>();

  private getStream(path: string): StoredStream | undefined {
    const stream = this.streams.get(path);
    if (!stream) {
      return;
    }

    if (isMetadataExpired(stream.metadata)) {
      return this.expireStream(path, stream);
    }

    return stream;
  }

  private getLiveStream(path: string): StoredStream | undefined {
    const stream = this.getStream(path);
    if (!stream) {
      return;
    }
    assertStreamLive(path, stream.metadata);
    return stream;
  }

  private touchStream(path: string, stream: StoredStream): void {
    if (stream.metadata.ttlSeconds === undefined) {
      return;
    }
    stream.metadata = { ...stream.metadata, lastAccessedAt: Date.now() };
    this.streams.set(path, stream);
  }

  private expireStream(
    path: string,
    stream: StoredStream
  ): StoredStream | undefined {
    if ((stream.metadata.childCount ?? 0) > 0) {
      stream.metadata = { ...stream.metadata, deleted: true };
      this.notifyDeleted(stream);
      return stream;
    }

    this.hardDelete(path, stream);
    return;
  }

  private hardDelete(path: string, stream: StoredStream): void {
    this.notifyDeleted(stream);
    this.streams.delete(path);
    this.releaseParent(stream.metadata.forkedFrom);
  }

  private releaseParent(parentPath: string | undefined): void {
    if (!parentPath) {
      return;
    }

    const parent = this.streams.get(parentPath);
    if (!parent) {
      return;
    }

    const childCount = Math.max(0, (parent.metadata.childCount ?? 0) - 1);
    parent.metadata = { ...parent.metadata, childCount };

    if (parent.metadata.deleted === true && childCount === 0) {
      this.hardDelete(parentPath, parent);
    }
  }

  put(path: string, options: PutOptions): Promise<PutResult> {
    const existing = this.getStream(path);

    if (existing) {
      if (existing.metadata.deleted === true) {
        throw new StreamConflictError("stream is gone");
      }
      validateIdempotentCreate(existing.metadata, options);
      return Promise.resolve({
        created: false,
        nextOffset: existing.nextOffset,
        closed: existing.closed,
      });
    }

    let contentType = options.contentType;
    let ttlSeconds = options.ttlSeconds;
    let expiresAt = options.expiresAt;
    let closed = options.closed === true;
    let forkedFrom: string | undefined;
    let forkOffset: Offset | undefined;
    let prepared = prepareInitialData(options);

    if (options.forkedFrom !== undefined) {
      const source = this.getStream(options.forkedFrom);
      if (!source) {
        throw new StreamNotFoundError(options.forkedFrom);
      }
      if (source.metadata.deleted === true) {
        throw new StreamConflictError("fork source is gone");
      }
      validateAppendContentType(source.metadata.contentType, contentType);

      forkedFrom = options.forkedFrom;
      forkOffset = options.forkOffset ?? source.nextOffset;
      prepared = prepareForkData(source.data, forkOffset);
      ({ ttlSeconds, expiresAt } = inheritedExpiration(
        source.metadata,
        options
      ));
      contentType = source.metadata.contentType;
      closed = false;
      source.metadata = {
        ...source.metadata,
        childCount: (source.metadata.childCount ?? 0) + 1,
      };
    }

    const now = Date.now();
    const stream: StoredStream = {
      metadata: {
        path,
        contentType,
        ttlSeconds,
        expiresAt,
        createdAt: now,
        lastAccessedAt: now,
        forkedFrom,
        forkOffset,
        childCount: 0,
        deleted: false,
      },
      data: prepared.data,
      nextOffset: prepared.nextOffset,
      lastSeq: undefined,
      producers: {},
      appendCount: prepared.appendCount,
      closed,
      waiters: [],
    };

    this.streams.set(path, stream);

    return Promise.resolve({
      created: true,
      nextOffset: stream.nextOffset,
      closed: stream.closed,
    });
  }

  append(
    path: string,
    data: Uint8Array,
    options?: AppendOptions
  ): Promise<AppendResult> {
    const stream = this.getLiveStream(path);
    if (!stream) {
      throw new StreamNotFoundError(path);
    }

    const producerDecision = evaluateProducerAppend(
      stream.producers,
      options?.producer
    );
    const closedResult = closedAppendResult(
      path,
      stream.nextOffset,
      stream.closed,
      data,
      options,
      producerDecision
    );
    if (closedResult) {
      this.touchStream(path, stream);
      return Promise.resolve(closedResult);
    }

    if (data.length > 0) {
      validateAppendContentType(
        stream.metadata.contentType,
        options?.contentType
      );
    }

    if (producerDecision._tag === "Duplicate") {
      this.touchStream(path, stream);
      return Promise.resolve({
        nextOffset: stream.nextOffset,
        producer: producerDecision.result,
        closed: stream.closed,
        appended: false,
      });
    }
    validateAppendSeq(stream.lastSeq, options?.seq);

    const append = prepareAppendData(
      stream.data,
      data,
      stream.metadata.contentType,
      stream.appendCount,
      stream.nextOffset
    );
    if (options?.seq !== undefined) {
      stream.lastSeq = options.seq;
    }
    stream.producers = commitProducerAppend(stream.producers, producerDecision);

    stream.data = append.data;
    stream.appendCount = append.appendCount;
    stream.nextOffset = append.nextOffset;
    stream.closed = options?.close === true;
    this.touchStream(path, stream);

    this.notifyWaiters(stream);

    return Promise.resolve(
      appendResult(
        stream.nextOffset,
        stream.closed,
        append.appended,
        producerDecision
      )
    );
  }

  get(path: string, options?: GetOptions): Promise<GetResult> {
    const stream = this.getLiveStream(path);
    if (!stream) {
      return Promise.reject(new StreamNotFoundError(path));
    }
    this.touchStream(path, stream);

    const startOffset = options?.offset ?? initialOffset();
    const byteOffset = offsetToBytePos(startOffset);

    const messages: StreamMessage[] = [];

    if (byteOffset < stream.data.length) {
      const data = stream.data.slice(byteOffset);
      messages.push({
        offset: startOffset,
        timestamp: Date.now(),
        data,
      });
    }

    return Promise.resolve({
      messages,
      nextOffset: stream.nextOffset,
      upToDate: true,
      cursor: calculateCursor(),
      etag: generateETag(path, startOffset, stream.nextOffset),
      contentType: stream.metadata.contentType,
      closed: stream.closed,
      ttlSeconds: stream.metadata.ttlSeconds,
      expiresAt: stream.metadata.expiresAt,
    });
  }

  head(path: string): Promise<HeadResult | null> {
    const stream = this.getStream(path);
    if (!stream) {
      return Promise.resolve(null);
    }
    assertStreamLive(path, stream.metadata);

    return Promise.resolve({
      contentType: stream.metadata.contentType,
      nextOffset: stream.nextOffset,
      etag: generateETag(path, initialOffset(), stream.nextOffset),
      closed: stream.closed,
      ttlSeconds: stream.metadata.ttlSeconds,
      expiresAt: stream.metadata.expiresAt,
    });
  }

  delete(path: string): Promise<void> {
    const stream = this.getStream(path);
    if (!stream) {
      return Promise.resolve();
    }

    assertStreamLive(path, stream.metadata);

    if ((stream.metadata.childCount ?? 0) > 0) {
      stream.metadata = { ...stream.metadata, deleted: true };
      this.notifyDeleted(stream);
      return Promise.resolve();
    }

    this.hardDelete(path, stream);
    return Promise.resolve();
  }

  has(path: string): boolean {
    const stream = this.getStream(path);
    return stream !== undefined && stream.metadata.deleted !== true;
  }

  waitForData(
    path: string,
    offset: Offset,
    timeoutMs: number
  ): Promise<WaitResult> {
    const stream = this.getLiveStream(path);
    if (!stream) {
      return Promise.reject(new StreamNotFoundError(path));
    }
    this.touchStream(path, stream);

    const byteOffset = offsetToBytePos(offset);

    if (byteOffset < stream.data.length) {
      const data = stream.data.slice(byteOffset);
      return Promise.resolve({
        messages: [{ offset, timestamp: Date.now(), data }],
        timedOut: false,
        closed: stream.closed,
      });
    }

    if (stream.closed) {
      return Promise.resolve({ messages: [], timedOut: false, closed: true });
    }

    const effect = Effect.gen(this, function* () {
      const deferred = yield* Deferred.make<WaitResult>();
      const waiter: Waiter = { deferred, offset };
      stream.waiters.push(waiter);

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

      const index = stream.waiters.indexOf(waiter);
      if (index !== -1) {
        stream.waiters.splice(index, 1);
      }

      return result;
    });

    return Effect.runPromise(effect);
  }

  formatResponse(path: string, messages: StreamMessage[]): Uint8Array {
    const stream = this.getStream(path);
    if (!stream) {
      return new Uint8Array(0);
    }

    if (messages.length === 0) {
      const isJson = isJsonContentType(stream.metadata.contentType);
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

    const isJson = isJsonContentType(stream.metadata.contentType);
    return isJson ? formatJsonResponse(combined) : combined;
  }

  private notifyWaiters(stream: StoredStream): void {
    const waiters = [...stream.waiters];
    stream.waiters = [];

    const effect = Effect.forEach(waiters, (waiter) => {
      const byteOffset = offsetToBytePos(waiter.offset);

      if (byteOffset < stream.data.length) {
        const data = stream.data.slice(byteOffset);
        return Deferred.succeed(waiter.deferred, {
          messages: [{ offset: waiter.offset, timestamp: Date.now(), data }],
          timedOut: false,
          closed: stream.closed,
        });
      }
      if (stream.closed) {
        return Deferred.succeed(waiter.deferred, {
          messages: [],
          timedOut: false,
          closed: true,
        });
      }
      stream.waiters.push(waiter);
      return Effect.void;
    });

    Effect.runSync(effect);
  }

  private notifyDeleted(stream: StoredStream): void {
    const effect = Effect.forEach(stream.waiters, (waiter) =>
      Deferred.succeed(waiter.deferred, { messages: [], timedOut: false })
    );
    Effect.runSync(effect);
    stream.waiters = [];
  }
}
