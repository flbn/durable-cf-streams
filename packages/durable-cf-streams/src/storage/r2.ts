import { calculateCursor } from "../cursor.js";
import { StreamConflictError, StreamNotFoundError } from "../errors.js";
import { initialOffset, offsetToBytePos } from "../offsets.js";
import {
  commitProducerAppend,
  evaluateProducerAppend,
  type ProducerAppendDecision,
} from "../producer.js";
import {
  formatJsonResponse,
  generateETag,
  isExpired,
  isJsonContentType,
} from "../protocol.js";
import {
  decodePersistedStreamMetadataJson,
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
  decodeProducerAppendReceipt,
  encodeProducerAppendReceipt,
  generateIncarnation,
  type PreparedWholeValueCreate,
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

type R2StreamMetadata = PersistedStreamMetadata;

type PreparedCreate = {
  readonly prepared: PreparedWholeValueCreate;
  readonly forkedFrom?: string;
  readonly forkSource?: R2StreamMetadata;
};

export type R2StoreOptions = {
  /** NOTE: R2 has no compare-and-set append primitive; callers route each stream path through one serialized owner, including TTL-renewing reads. */
  readonly serializedOwner: true;
  readonly maxReadBytes?: number;
  readonly maxAppendBytes?: number;
  readonly maxStreamBytes?: number;
};

export const DEFAULT_R2_MAX_READ_BYTES = 1_000_000;
export const DEFAULT_R2_MAX_APPEND_BYTES = 12 * 1024 * 1024;
export const DEFAULT_R2_MAX_STREAM_BYTES = DEFAULT_R2_MAX_APPEND_BYTES;

export class R2Store implements StreamStore {
  private readonly bucket: R2Bucket;
  private readonly waiters = new Map<string, Waiter[]>();
  private readonly streamCache = new Map<string, { contentType: string }>();
  private readonly maxReadBytes: number;
  private readonly maxAppendBytes: number;
  private readonly maxStreamBytes: number;

  constructor(bucket: R2Bucket, options: R2StoreOptions) {
    if (options.serializedOwner !== true) {
      throw new StreamConflictError(
        "R2Store requires one serialized owner per stream path"
      );
    }
    this.bucket = bucket;
    this.maxReadBytes = options?.maxReadBytes ?? DEFAULT_R2_MAX_READ_BYTES;
    this.maxAppendBytes =
      options?.maxAppendBytes ?? DEFAULT_R2_MAX_APPEND_BYTES;
    this.maxStreamBytes =
      options?.maxStreamBytes ?? DEFAULT_R2_MAX_STREAM_BYTES;
  }

  private metaKey(path: string): string {
    return `stream/${path}/meta.json`;
  }

  private dataKey(path: string): string {
    return `stream/${path}/data`;
  }

  private producerAppendKey(
    path: string,
    producer: NonNullable<AppendOptions["producer"]>
  ): string {
    return `stream/${path}/producer/${producerAppendReceiptKey(producer)}`;
  }

  private producerAppendPrefix(path: string): string {
    return `stream/${path}/producer/`;
  }

  private async getMetadata(path: string): Promise<R2StreamMetadata | null> {
    const obj = await this.bucket.get(this.metaKey(path));
    if (!obj) {
      return null;
    }

    return decodePersistedStreamMetadataJson(await obj.text());
  }

  private async getData(path: string): Promise<Uint8Array> {
    const obj = await this.bucket.get(this.dataKey(path));
    if (!obj) {
      return new Uint8Array(0);
    }

    const buffer = await obj.arrayBuffer();
    return new Uint8Array(buffer);
  }

  private async getVisibleData(
    path: string,
    meta: R2StreamMetadata
  ): Promise<Uint8Array> {
    const visibleLength = offsetToBytePos(meta.nextOffset);
    assertPayloadSize(this.maxStreamBytes, visibleLength);
    return (await this.getData(path)).slice(0, visibleLength);
  }

  private async putMetadata(
    path: string,
    meta: R2StreamMetadata
  ): Promise<void> {
    await this.bucket.put(this.metaKey(path), JSON.stringify(meta), {
      httpMetadata: { contentType: "application/json" },
    });
  }

  private async getStreamMetadata(
    path: string,
    options?: { readonlyExpiration?: boolean }
  ): Promise<R2StreamMetadata | null> {
    const meta = await this.getMetadata(path);
    if (!meta) {
      return null;
    }

    if (isExpired(meta)) {
      if (options?.readonlyExpiration === true) {
        return (meta.childCount ?? 0) > 0 ? { ...meta, deleted: true } : null;
      }
      return await this.expireStream(path, meta);
    }

    return meta;
  }

  private async touchMetadata(
    path: string,
    meta: R2StreamMetadata
  ): Promise<R2StreamMetadata> {
    if (meta.ttlSeconds === undefined) {
      return meta;
    }
    const updated = { ...meta, lastAccessedAt: Date.now() };
    await this.putMetadata(path, updated);
    return updated;
  }

  private async expireStream(
    path: string,
    meta: R2StreamMetadata
  ): Promise<R2StreamMetadata | null> {
    if ((meta.childCount ?? 0) > 0) {
      const updated = { ...meta, deleted: true };
      await this.putMetadata(path, updated);
      this.notifyDeleted(path);
      return updated;
    }

    await this.hardDelete(path, meta);
    return null;
  }

  private async hardDelete(
    path: string,
    meta: R2StreamMetadata
  ): Promise<void> {
    this.notifyDeleted(path);
    await this.deleteProducerAppendReceipts(path);
    await Promise.all([
      this.bucket.delete(this.metaKey(path)),
      this.bucket.delete(this.dataKey(path)),
    ]);
    await this.releaseParent(meta.forkedFrom);
  }

  private async deleteProducerAppendReceipts(path: string): Promise<void> {
    let cursor: string | undefined;
    do {
      const listed = await this.bucket.list({
        prefix: this.producerAppendPrefix(path),
        cursor,
      });
      await Promise.all(
        listed.objects.map((object) => this.bucket.delete(object.key))
      );
      cursor = listed.truncated ? listed.cursor : undefined;
    } while (cursor !== undefined);
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

  private existingCreateResult(
    path: string,
    existingMeta: R2StreamMetadata | null,
    options: PutOptions
  ): PutResult | undefined {
    if (!existingMeta) {
      return;
    }
    if (existingMeta.deleted === true) {
      throw new StreamConflictError("stream is gone");
    }
    assertStreamIncarnation(
      path,
      existingMeta.incarnation,
      options.expectedIncarnation
    );
    validateIdempotentCreate(existingMeta, options);
    return {
      created: false,
      incarnation: existingMeta.incarnation,
      nextOffset: existingMeta.nextOffset,
      contentType: existingMeta.contentType,
      closed: existingMeta.closed,
    };
  }

  private async prepareCreate(
    path: string,
    options: PutOptions
  ): Promise<PreparedCreate> {
    if (options.expectedIncarnation !== undefined) {
      throw new StreamConflictError(`stream incarnation is stale: ${path}`);
    }

    if (options.forkedFrom === undefined) {
      return {
        prepared: prepareWholeValueCreate(
          options,
          this.maxAppendBytes,
          this.maxStreamBytes
        ),
      };
    }

    const source = await this.getStreamMetadata(options.forkedFrom);
    if (!source) {
      throw new StreamNotFoundError(options.forkedFrom);
    }
    if (source.deleted === true) {
      throw new StreamConflictError("fork source is gone");
    }
    const sourceData = await this.getVisibleData(options.forkedFrom, source);

    return {
      prepared: prepareWholeValueForkCreate(
        {
          ...source,
          data: sourceData,
        },
        options,
        this.maxAppendBytes,
        this.maxStreamBytes
      ),
      forkedFrom: options.forkedFrom,
      forkSource: source,
    };
  }

  async put(path: string, options: PutOptions): Promise<PutResult> {
    const existingResult = this.existingCreateResult(
      path,
      await this.getStreamMetadata(path),
      options
    );
    if (existingResult) {
      return existingResult;
    }

    const { prepared, forkedFrom, forkSource } = await this.prepareCreate(
      path,
      options
    );

    const now = Date.now();
    const meta: R2StreamMetadata = {
      incarnation: generateIncarnation(),
      contentType: prepared.contentType,
      ttlSeconds: prepared.ttlSeconds,
      expiresAt: prepared.expiresAt,
      createdAt: now,
      lastAccessedAt: now,
      nextOffset: prepared.nextOffset,
      appendCount: prepared.appendCount,
      appendEndPositions: prepared.appendEndPositions,
      producers: {},
      closed: prepared.closed,
      forkedFrom,
      forkOffset: prepared.forkOffset,
      forkSubOffset: prepared.forkSubOffset,
      childCount: 0,
      deleted: false,
    };

    try {
      await this.bucket.put(this.dataKey(path), prepared.data);
      await this.putMetadata(path, meta);
      if (forkedFrom !== undefined && forkSource !== undefined) {
        await this.putMetadata(forkedFrom, {
          ...forkSource,
          childCount: (forkSource.childCount ?? 0) + 1,
        });
      }
    } catch (error) {
      await Promise.all([
        this.bucket.delete(this.metaKey(path)),
        this.bucket.delete(this.dataKey(path)),
      ]);
      throw error;
    }

    this.streamCache.set(path, { contentType: prepared.contentType });

    return {
      created: true,
      incarnation: meta.incarnation,
      nextOffset: meta.nextOffset,
      contentType: meta.contentType,
      closed: meta.closed,
    };
  }

  async append(
    path: string,
    data: Uint8Array,
    options?: AppendOptions
  ): Promise<AppendResult> {
    const meta = await this.getStreamMetadata(path);

    if (!meta) {
      throw new StreamNotFoundError(path);
    }
    assertStreamLive(path, meta);
    assertStreamIncarnation(
      path,
      meta.incarnation,
      options?.expectedIncarnation
    );

    const producers = meta.producers;
    const producerDecision = evaluateProducerAppend(
      producers,
      options?.producer
    );
    if (data.length > 0) {
      validateAppendContentType(meta.contentType, options?.contentType);
    }
    const appendPayload = prepareAppendPayload(data, meta.contentType);
    assertPayloadSize(this.maxAppendBytes, appendPayload.length);
    const shouldClose = options?.close === true;

    const duplicateResult = await this.duplicateAppendResult(
      path,
      meta,
      options?.producer,
      appendPayload,
      shouldClose,
      producerDecision
    );
    if (duplicateResult) {
      return duplicateResult;
    }
    const closedResult = closedAppendResult(
      path,
      meta.incarnation,
      meta.nextOffset,
      meta.closed === true,
      data,
      options,
      producerDecision
    );
    if (closedResult) {
      return closedResult;
    }
    validateAppendSeq(meta.lastSeq, options?.seq);

    const existingData = await this.getVisibleData(path, meta);

    const append = prepareAppendData(
      existingData,
      data,
      meta.contentType,
      meta.appendCount,
      meta.nextOffset
    );
    assertPayloadSize(this.maxStreamBytes, append.data.length);
    const appendEndPositions = [...(meta.appendEndPositions ?? [])];
    if (append.appended) {
      appendEndPositions.push(offsetToBytePos(append.nextOffset));
    }
    const updatedMeta: R2StreamMetadata = {
      ...meta,
      nextOffset: append.nextOffset,
      lastSeq: options?.seq ?? meta.lastSeq,
      appendCount: append.appendCount,
      appendEndPositions,
      producers: commitProducerAppend(producers, producerDecision),
      closed: shouldClose,
      lastAccessedAt: Date.now(),
    };

    await this.bucket.put(this.dataKey(path), append.data);
    if (producerDecision._tag === "Accepted") {
      await this.bucket.put(
        this.producerAppendKey(path, producerDecision.result),
        encodeProducerAppendReceipt({
          endOffset: append.nextOffset,
          data: appendPayload,
          closed: shouldClose,
        })
      );
    }
    await this.putMetadata(path, updatedMeta);

    this.notifyWaiters(
      path,
      append.data,
      updatedMeta.nextOffset,
      shouldClose,
      updatedMeta.contentType,
      appendEndPositions
    );

    return appendResult(
      updatedMeta.incarnation,
      updatedMeta.nextOffset,
      updatedMeta.closed === true,
      append.appended,
      producerDecision
    );
  }

  private async duplicateAppendResult(
    path: string,
    meta: R2StreamMetadata,
    producer: AppendOptions["producer"],
    appendPayload: Uint8Array,
    shouldClose: boolean,
    producerDecision: ProducerAppendDecision
  ): Promise<AppendResult | undefined> {
    if (producerDecision._tag !== "Duplicate") {
      return;
    }
    if (producer === undefined) {
      throw new StreamConflictError(
        `producer retry receipt is missing: ${path}`
      );
    }
    const receiptObject = await this.bucket.get(
      this.producerAppendKey(path, producer)
    );
    const receipt = assertProducerAppendReceiptMatches(
      path,
      decodeProducerAppendReceipt(
        path,
        receiptObject === null
          ? null
          : new Uint8Array(await receiptObject.arrayBuffer())
      ),
      appendPayload,
      shouldClose
    );
    return {
      incarnation: meta.incarnation,
      nextOffset: receipt.endOffset,
      producer: producerDecision.result,
      closed: meta.closed,
      appended: false,
    };
  }

  async get(path: string, options?: GetOptions): Promise<GetResult> {
    let meta = await this.getStreamMetadata(path, {
      readonlyExpiration: true,
    });

    if (!meta) {
      this.streamCache.delete(path);
      throw new StreamNotFoundError(path);
    }
    assertStreamLive(path, meta);
    assertStreamIncarnation(
      path,
      meta.incarnation,
      options?.expectedIncarnation
    );
    if (options?.renewTtl !== false) {
      meta = await this.touchMetadata(path, meta);
    }
    this.streamCache.set(path, { contentType: meta.contentType });

    const data = await this.getVisibleData(path, meta);

    const startOffset = options?.offset ?? initialOffset();
    const window = readWholeValueWindow(
      data,
      startOffset,
      meta.nextOffset,
      meta.closed,
      this.maxReadBytes,
      meta.contentType,
      meta.appendEndPositions ?? []
    );

    return {
      messages: window.messages,
      incarnation: meta.incarnation,
      nextOffset: window.nextOffset,
      upToDate: window.upToDate,
      cursor: calculateCursor(),
      etag: generateETag(path, startOffset, window.nextOffset),
      contentType: meta.contentType,
      closed: window.closed === true,
    };
  }

  async head(path: string): Promise<HeadResult | null> {
    const meta = await this.getStreamMetadata(path, {
      readonlyExpiration: true,
    });

    if (!meta) {
      this.streamCache.delete(path);
      return null;
    }
    assertStreamLive(path, meta);

    this.streamCache.set(path, { contentType: meta.contentType });

    return {
      contentType: meta.contentType,
      incarnation: meta.incarnation,
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

  async has(path: string): Promise<boolean> {
    const meta = await this.getStreamMetadata(path, {
      readonlyExpiration: true,
    });
    return meta !== null && meta !== undefined && meta.deleted !== true;
  }

  async waitForData(
    path: string,
    offset: Offset,
    timeoutMs: number,
    options?: WaitOptions
  ): Promise<WaitResult> {
    const meta = await this.getStreamMetadata(path, {
      readonlyExpiration: true,
    });

    if (!meta) {
      throw new StreamNotFoundError(path);
    }
    assertStreamLive(path, meta);
    assertStreamIncarnation(
      path,
      meta.incarnation,
      options?.expectedIncarnation
    );
    const data = await this.getVisibleData(path, meta);

    const window = readWholeValueWindow(
      data,
      offset,
      meta.nextOffset,
      meta.closed,
      this.maxReadBytes,
      meta.contentType,
      meta.appendEndPositions ?? []
    );

    if (window.messages.length > 0) {
      return {
        messages: window.messages,
        timedOut: false,
        incarnation: meta.incarnation,
        closed: window.closed,
      };
    }

    if (meta.closed === true) {
      return {
        messages: [],
        timedOut: false,
        incarnation: meta.incarnation,
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

  private notifyWaiters(
    path: string,
    data: Uint8Array,
    tailOffset: Offset,
    closed = false,
    contentType?: string,
    appendEndPositions: readonly number[] = []
  ): void {
    const waiters = this.waiters.get(path) ?? [];
    this.waiters.set(path, []);
    notifyDataWaiters(
      waiters,
      data,
      tailOffset,
      closed,
      this.maxReadBytes,
      contentType,
      appendEndPositions,
      (waiter) => {
        const remaining = this.waiters.get(path) ?? [];
        remaining.push(waiter);
        this.waiters.set(path, remaining);
      }
    );
  }

  private notifyDeleted(path: string): void {
    const waiters = this.waiters.get(path) ?? [];
    notifyDeletedWaiters(waiters);

    this.waiters.delete(path);
    this.streamCache.delete(path);
  }
}
