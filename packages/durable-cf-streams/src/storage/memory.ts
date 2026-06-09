import { Deferred, Effect } from "effect";
import { calculateCursor } from "../cursor.js";
import { StreamNotFoundError } from "../errors.js";
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
  closedAppendResult,
  prepareAppendData,
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
      this.streams.delete(path);
      return;
    }

    return stream;
  }

  put(path: string, options: PutOptions): Promise<PutResult> {
    const existing = this.getStream(path);

    if (existing) {
      validateIdempotentCreate(existing.metadata, options);
      return Promise.resolve({
        created: false,
        nextOffset: existing.nextOffset,
        closed: existing.closed,
      });
    }

    const { data, appendCount, nextOffset } = prepareInitialData(options);

    const stream: StoredStream = {
      metadata: {
        path,
        contentType: options.contentType,
        ttlSeconds: options.ttlSeconds,
        expiresAt: options.expiresAt,
        createdAt: Date.now(),
      },
      data,
      nextOffset,
      lastSeq: undefined,
      producers: {},
      appendCount,
      closed: options.closed === true,
      waiters: [],
    };

    this.streams.set(path, stream);

    return Promise.resolve({
      created: true,
      nextOffset,
      closed: stream.closed,
    });
  }

  append(
    path: string,
    data: Uint8Array,
    options?: AppendOptions
  ): Promise<AppendResult> {
    const stream = this.getStream(path);
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
      return Promise.resolve(closedResult);
    }

    if (data.length > 0) {
      validateAppendContentType(
        stream.metadata.contentType,
        options?.contentType
      );
    }

    if (producerDecision._tag === "Duplicate") {
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
    const stream = this.getStream(path);
    if (!stream) {
      return Promise.reject(new StreamNotFoundError(path));
    }

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
    });
  }

  head(path: string): Promise<HeadResult | null> {
    const stream = this.getStream(path);
    if (!stream) {
      return Promise.resolve(null);
    }

    return Promise.resolve({
      contentType: stream.metadata.contentType,
      nextOffset: stream.nextOffset,
      etag: generateETag(path, initialOffset(), stream.nextOffset),
      closed: stream.closed,
    });
  }

  delete(path: string): Promise<void> {
    const stream = this.streams.get(path);
    if (stream) {
      const effect = Effect.forEach(stream.waiters, (waiter) =>
        Deferred.succeed(waiter.deferred, { messages: [], timedOut: false })
      );
      Effect.runSync(effect);
    }
    this.streams.delete(path);
    return Promise.resolve();
  }

  has(path: string): boolean {
    return this.getStream(path) !== undefined;
  }

  waitForData(
    path: string,
    offset: Offset,
    timeoutMs: number
  ): Promise<WaitResult> {
    const stream = this.getStream(path);
    if (!stream) {
      return Promise.reject(new StreamNotFoundError(path));
    }

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
}
