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
  WaitOptions,
  WaitResult,
} from "../types.js";
import type { StreamStore } from "./interface.js";
import {
  appendResult,
  assertPayloadSize,
  assertProducerAppendReceiptMatches,
  assertStreamIncarnation,
  assertStreamLive,
  closedAppendResult,
  generateIncarnation,
  type ProducerAppendReceipt,
  prepareAppendData,
  prepareAppendPayload,
  prepareWholeValueCreate,
  prepareWholeValueForkCreate,
  producerAppendReceiptKey,
  readWholeValueWindow,
  validateAppendContentType,
  validateAppendSeq,
  validateIdempotentCreate,
} from "./utils.js";
import {
  notifyDataWaiters,
  notifyDeletedWaiters,
  type Waiter,
  waitForChange,
} from "./waiters.js";

type StoredStream = {
  metadata: StreamMetadata;
  data: Uint8Array;
  nextOffset: Offset;
  lastSeq: string | undefined;
  producers: ProducerStateMap;
  producerAppends: Record<string, ProducerAppendReceipt>;
  appendEndPositions: number[];
  appendCount: number;
  closed: boolean;
  waiters: Waiter[];
};

export type MemoryStoreOptions = {
  readonly maxReadBytes?: number;
  readonly maxAppendBytes?: number;
  readonly maxStreamBytes?: number;
};

export const DEFAULT_MEMORY_MAX_READ_BYTES = 1_000_000;
export const DEFAULT_MEMORY_MAX_APPEND_BYTES = 12 * 1024 * 1024;
export const DEFAULT_MEMORY_MAX_STREAM_BYTES = DEFAULT_MEMORY_MAX_APPEND_BYTES;

export class MemoryStore implements StreamStore {
  private readonly streams = new Map<string, StoredStream>();
  private readonly maxReadBytes: number;
  private readonly maxAppendBytes: number;
  private readonly maxStreamBytes: number;

  constructor(options?: MemoryStoreOptions) {
    this.maxReadBytes = options?.maxReadBytes ?? DEFAULT_MEMORY_MAX_READ_BYTES;
    this.maxAppendBytes =
      options?.maxAppendBytes ?? DEFAULT_MEMORY_MAX_APPEND_BYTES;
    this.maxStreamBytes =
      options?.maxStreamBytes ?? DEFAULT_MEMORY_MAX_STREAM_BYTES;
  }

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

  private existingCreateResult(
    path: string,
    existing: StoredStream | undefined,
    options: PutOptions
  ): PutResult | undefined {
    if (!existing) {
      return;
    }
    if (existing.metadata.deleted === true) {
      throw new StreamConflictError("stream is gone");
    }
    assertStreamIncarnation(
      path,
      existing.metadata.incarnation,
      options.expectedIncarnation
    );
    validateIdempotentCreate(existing.metadata, options);
    return {
      created: false,
      incarnation: existing.metadata.incarnation,
      nextOffset: existing.nextOffset,
      contentType: existing.metadata.contentType,
      closed: existing.closed,
    };
  }

  put(path: string, options: PutOptions): Promise<PutResult> {
    const existingResult = this.existingCreateResult(
      path,
      this.getStream(path),
      options
    );
    if (existingResult) {
      return Promise.resolve(existingResult);
    }

    if (options.expectedIncarnation !== undefined) {
      throw new StreamConflictError(`stream incarnation is stale: ${path}`);
    }

    let forkedFrom: string | undefined;
    let prepared = prepareWholeValueCreate(
      options,
      this.maxAppendBytes,
      this.maxStreamBytes
    );

    if (options.forkedFrom !== undefined) {
      const source = this.getStream(options.forkedFrom);
      if (!source) {
        throw new StreamNotFoundError(options.forkedFrom);
      }
      if (source.metadata.deleted === true) {
        throw new StreamConflictError("fork source is gone");
      }

      forkedFrom = options.forkedFrom;
      prepared = prepareWholeValueForkCreate(
        {
          ...source.metadata,
          data: source.data,
          nextOffset: source.nextOffset,
          appendEndPositions: source.appendEndPositions,
        },
        options,
        this.maxAppendBytes,
        this.maxStreamBytes
      );
      source.metadata = {
        ...source.metadata,
        childCount: (source.metadata.childCount ?? 0) + 1,
      };
    }

    const now = Date.now();
    const stream: StoredStream = {
      metadata: {
        path,
        incarnation: generateIncarnation(),
        contentType: prepared.contentType,
        ttlSeconds: prepared.ttlSeconds,
        expiresAt: prepared.expiresAt,
        createdAt: now,
        lastAccessedAt: now,
        forkedFrom,
        forkOffset: prepared.forkOffset,
        forkSubOffset: prepared.forkSubOffset,
        childCount: 0,
        deleted: false,
      },
      data: prepared.data,
      nextOffset: prepared.nextOffset,
      lastSeq: undefined,
      producers: {},
      producerAppends: {},
      appendEndPositions: prepared.appendEndPositions,
      appendCount: prepared.appendCount,
      closed: prepared.closed,
      waiters: [],
    };

    this.streams.set(path, stream);

    return Promise.resolve({
      created: true,
      incarnation: stream.metadata.incarnation,
      nextOffset: stream.nextOffset,
      contentType: stream.metadata.contentType,
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
    assertStreamIncarnation(
      path,
      stream.metadata.incarnation,
      options?.expectedIncarnation
    );

    const producerDecision = evaluateProducerAppend(
      stream.producers,
      options?.producer
    );
    if (data.length > 0) {
      validateAppendContentType(
        stream.metadata.contentType,
        options?.contentType
      );
    }
    const appendPayload = prepareAppendPayload(
      data,
      stream.metadata.contentType
    );
    assertPayloadSize(this.maxAppendBytes, appendPayload.length);
    const shouldClose = options?.close === true;

    if (producerDecision._tag === "Duplicate") {
      const receipt = assertProducerAppendReceiptMatches(
        path,
        options?.producer
          ? stream.producerAppends[producerAppendReceiptKey(options.producer)]
          : undefined,
        appendPayload,
        shouldClose
      );
      this.touchStream(path, stream);
      return Promise.resolve({
        incarnation: stream.metadata.incarnation,
        nextOffset: receipt.endOffset,
        producer: producerDecision.result,
        closed: stream.closed,
        appended: false,
      });
    }
    const closedResult = closedAppendResult(
      path,
      stream.metadata.incarnation,
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
    validateAppendSeq(stream.lastSeq, options?.seq);

    const append = prepareAppendData(
      stream.data,
      data,
      stream.metadata.contentType,
      stream.appendCount,
      stream.nextOffset
    );
    assertPayloadSize(this.maxStreamBytes, append.data.length);
    if (options?.seq !== undefined) {
      stream.lastSeq = options.seq;
    }
    stream.producers = commitProducerAppend(stream.producers, producerDecision);
    if (producerDecision._tag === "Accepted") {
      stream.producerAppends[
        producerAppendReceiptKey(producerDecision.result)
      ] = {
        endOffset: append.nextOffset,
        data: appendPayload,
        closed: shouldClose,
      };
    }

    stream.data = append.data;
    stream.appendCount = append.appendCount;
    stream.nextOffset = append.nextOffset;
    if (append.appended) {
      stream.appendEndPositions.push(offsetToBytePos(append.nextOffset));
    }
    stream.closed = shouldClose;
    this.touchStream(path, stream);

    this.notifyWaiters(stream);

    return Promise.resolve(
      appendResult(
        stream.metadata.incarnation,
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
    assertStreamIncarnation(
      path,
      stream.metadata.incarnation,
      options?.expectedIncarnation
    );
    if (options?.renewTtl !== false) {
      this.touchStream(path, stream);
    }
    assertPayloadSize(this.maxStreamBytes, stream.data.length);

    const startOffset = options?.offset ?? initialOffset();
    const window = readWholeValueWindow(
      stream.data,
      startOffset,
      stream.nextOffset,
      stream.closed,
      this.maxReadBytes,
      stream.metadata.contentType,
      stream.appendEndPositions
    );

    return Promise.resolve({
      messages: window.messages,
      incarnation: stream.metadata.incarnation,
      nextOffset: window.nextOffset,
      upToDate: window.upToDate,
      cursor: calculateCursor(),
      etag: generateETag(path, startOffset, window.nextOffset),
      contentType: stream.metadata.contentType,
      closed: window.closed === true,
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
      incarnation: stream.metadata.incarnation,
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

  has(path: string): Promise<boolean> {
    const stream = this.getStream(path);
    return Promise.resolve(
      stream !== undefined && stream.metadata.deleted !== true
    );
  }

  waitForData(
    path: string,
    offset: Offset,
    timeoutMs: number,
    options?: WaitOptions
  ): Promise<WaitResult> {
    const stream = this.getLiveStream(path);
    if (!stream) {
      return Promise.reject(new StreamNotFoundError(path));
    }
    assertStreamIncarnation(
      path,
      stream.metadata.incarnation,
      options?.expectedIncarnation
    );
    if (options?.renewTtl !== false) {
      this.touchStream(path, stream);
    }
    assertPayloadSize(this.maxStreamBytes, stream.data.length);

    const window = readWholeValueWindow(
      stream.data,
      offset,
      stream.nextOffset,
      stream.closed,
      this.maxReadBytes,
      stream.metadata.contentType,
      stream.appendEndPositions
    );

    if (window.messages.length > 0) {
      return Promise.resolve({
        messages: window.messages,
        timedOut: false,
        incarnation: stream.metadata.incarnation,
        closed: window.closed,
      });
    }

    if (stream.closed) {
      return Promise.resolve({
        messages: [],
        timedOut: false,
        incarnation: stream.metadata.incarnation,
        closed: true,
      });
    }

    return waitForChange(
      {
        add: (waiter) => stream.waiters.push(waiter),
        remove: (waiter) => {
          const index = stream.waiters.indexOf(waiter);
          if (index !== -1) {
            stream.waiters.splice(index, 1);
          }
        },
      },
      offset,
      timeoutMs
    );
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
    notifyDataWaiters(
      waiters,
      stream.data,
      stream.nextOffset,
      stream.closed,
      this.maxReadBytes,
      stream.metadata.contentType,
      stream.appendEndPositions,
      (waiter) => stream.waiters.push(waiter)
    );
  }

  private notifyDeleted(stream: StoredStream): void {
    notifyDeletedWaiters(stream.waiters);
    stream.waiters = [];
  }
}
