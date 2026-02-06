import type {
  AppendOptions,
  AppendResult,
  DurableStreamStore,
  ProducerState,
  ProducerValidationResult,
  Stream,
  StreamMessage,
} from "@durable-streams/server/types";
import { formatOffset, initialOffset, offsetToBytePos } from "../offsets.js";
import {
  formatJsonResponse,
  isExpired,
  isJsonContentType,
  isMetadataExpired,
} from "../protocol.js";
import {
  mergeData,
  prepareInitialData,
  validateAppendContentType,
  validateAppendSeq,
  validateIdempotentCreate,
} from "./utils.js";

const PRODUCER_STATE_TTL_MS = 7 * 24 * 60 * 60 * 1000;
const STREAM_KEY_PATTERN = /^stream:(.+):(meta|data|producers)$/;

type StreamRecord = {
  contentType: string;
  ttlSeconds?: number;
  expiresAt?: string;
  createdAt: number;
  nextOffset: string;
  lastSeq?: string;
  appendCount: number;
  closed: boolean;
  closedBy?: { producerId: string; epoch: number; seq: number };
};

type ProducerRecord = Record<
  string,
  { epoch: number; lastSeq: number; lastUpdated: number }
>;

type CachedStream = {
  contentType: string;
  closed: boolean;
  nextOffset: string;
  lastSeq?: string;
  ttlSeconds?: number;
  expiresAt?: string;
  createdAt: number;
};

type Waiter = {
  offset: string;
  resolve: (result: {
    messages: StreamMessage[];
    timedOut: boolean;
    streamClosed?: boolean;
  }) => void;
  timer: ReturnType<typeof setTimeout>;
};

export class KVStore implements DurableStreamStore {
  private readonly kv: KVNamespace;
  private readonly streamCache = new Map<string, CachedStream>();
  private readonly waiters = new Map<string, Waiter[]>();
  private readonly pathLocks = new Map<string, Promise<unknown>>();

  constructor(kv: KVNamespace) {
    this.kv = kv;
  }

  private metaKey(path: string): string {
    return `stream:${path}:meta`;
  }

  private dataKey(path: string): string {
    return `stream:${path}:data`;
  }

  private producersKey(path: string): string {
    return `stream:${path}:producers`;
  }

  private async getMetadata(path: string): Promise<StreamRecord | null> {
    const meta = await this.kv.get<StreamRecord>(this.metaKey(path), "json");
    if (!meta) {
      return null;
    }
    return meta;
  }

  private async getValidMetadata(path: string): Promise<StreamRecord | null> {
    const meta = await this.getMetadata(path);
    if (!meta) {
      return null;
    }
    if (isExpired(meta)) {
      this.streamCache.delete(path);
      return null;
    }
    this.updateCache(path, meta);
    return meta;
  }

  private updateCache(path: string, meta: StreamRecord): void {
    this.streamCache.set(path, {
      contentType: meta.contentType,
      closed: meta.closed,
      nextOffset: meta.nextOffset,
      lastSeq: meta.lastSeq,
      ttlSeconds: meta.ttlSeconds,
      expiresAt: meta.expiresAt,
      createdAt: meta.createdAt,
    });
  }

  private async getData(path: string): Promise<Uint8Array> {
    const raw = await this.kv.get(this.dataKey(path), "arrayBuffer");
    return raw ? new Uint8Array(raw) : new Uint8Array(0);
  }

  private async getProducers(
    path: string
  ): Promise<Map<string, ProducerState>> {
    const record = await this.kv.get<ProducerRecord>(
      this.producersKey(path),
      "json"
    );
    if (!record) {
      return new Map();
    }
    return new Map(Object.entries(record));
  }

  private async saveProducers(
    path: string,
    producers: Map<string, ProducerState>
  ): Promise<void> {
    const record: ProducerRecord = Object.fromEntries(producers);
    await this.kv.put(this.producersKey(path), JSON.stringify(record));
  }

  private async saveMeta(path: string, meta: StreamRecord): Promise<void> {
    await this.kv.put(this.metaKey(path), JSON.stringify(meta));
  }

  private buildStream(
    path: string,
    meta: StreamRecord,
    producers: Map<string, ProducerState>
  ): Stream {
    return {
      path,
      contentType: meta.contentType,
      messages: [],
      currentOffset: meta.nextOffset,
      lastSeq: meta.lastSeq,
      ttlSeconds: meta.ttlSeconds,
      expiresAt: meta.expiresAt,
      createdAt: meta.createdAt,
      producers,
      closed: meta.closed || undefined,
      closedBy: meta.closedBy,
    };
  }

  async has(path: string): Promise<boolean> {
    return (await this.getValidMetadata(path)) !== null;
  }

  async create(
    path: string,
    options: {
      contentType?: string;
      ttlSeconds?: number;
      expiresAt?: string;
      initialData?: Uint8Array;
      closed?: boolean;
    }
  ): Promise<Stream> {
    const existing = await this.getValidMetadata(path);

    if (existing) {
      validateIdempotentCreate(existing, options);
      const producers = await this.getProducers(path);
      return this.buildStream(path, existing, producers);
    }

    const { data, appendCount, nextOffset } = prepareInitialData({
      contentType: options.contentType,
      initialData: options.initialData,
    });

    const contentType = options.contentType ?? "application/octet-stream";

    const meta: StreamRecord = {
      contentType,
      ttlSeconds: options.ttlSeconds,
      expiresAt: options.expiresAt,
      createdAt: Date.now(),
      nextOffset,
      appendCount,
      closed: options.closed ?? false,
    };

    await Promise.all([
      this.saveMeta(path, meta),
      this.kv.put(this.dataKey(path), data),
    ]);

    this.updateCache(path, meta);
    return this.buildStream(path, meta, new Map());
  }

  get(path: string): Stream | undefined {
    const cached = this.streamCache.get(path);
    if (!cached) {
      return undefined;
    }
    if (isMetadataExpired(cached)) {
      this.streamCache.delete(path);
      return undefined;
    }
    return {
      path,
      contentType: cached.contentType,
      messages: [],
      currentOffset: cached.nextOffset,
      lastSeq: cached.lastSeq,
      ttlSeconds: cached.ttlSeconds,
      expiresAt: cached.expiresAt,
      createdAt: cached.createdAt,
      closed: cached.closed || undefined,
    };
  }

  async read(
    path: string,
    offset?: string
  ): Promise<{ messages: StreamMessage[]; upToDate: boolean }> {
    const meta = await this.getValidMetadata(path);
    if (!meta) {
      throw new Error(`Stream not found: ${path}`);
    }

    const data = await this.getData(path);
    const startOffset = offset ?? initialOffset();
    const byteOffset = offsetToBytePos(startOffset);
    const messages: StreamMessage[] = [];

    if (byteOffset < data.length) {
      messages.push({
        offset: meta.nextOffset,
        timestamp: Date.now(),
        data: data.slice(byteOffset),
      });
    }

    return { messages, upToDate: true };
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

    if (isJson) {
      return formatJsonResponse(combined);
    }

    return combined;
  }

  async waitForMessages(
    path: string,
    offset: string,
    timeoutMs: number
  ): Promise<{
    messages: StreamMessage[];
    timedOut: boolean;
    streamClosed?: boolean;
  }> {
    const meta = await this.getValidMetadata(path);
    if (!meta) {
      throw new Error(`Stream not found: ${path}`);
    }

    const data = await this.getData(path);
    const byteOffset = offsetToBytePos(offset);

    if (byteOffset < data.length) {
      return {
        messages: [
          {
            offset: meta.nextOffset,
            timestamp: Date.now(),
            data: data.slice(byteOffset),
          },
        ],
        timedOut: false,
      };
    }

    if (meta.closed) {
      return { messages: [], timedOut: false, streamClosed: true };
    }

    return new Promise((resolve) => {
      const timer = setTimeout(() => {
        this.removeWaiter(path, waiter);
        resolve({ messages: [], timedOut: true, streamClosed: undefined });
      }, timeoutMs);

      const waiter: Waiter = { offset, resolve, timer };
      const pathWaiters = this.waiters.get(path) ?? [];
      pathWaiters.push(waiter);
      this.waiters.set(path, pathWaiters);
    });
  }

  async append(
    path: string,
    data: Uint8Array,
    options?: AppendOptions
  ): Promise<StreamMessage | AppendResult> {
    const release = await this.acquirePathLock(path);
    try {
      return await this.appendLocked(path, data, options);
    } finally {
      release();
    }
  }

  private async appendLocked(
    path: string,
    data: Uint8Array,
    options?: AppendOptions
  ): Promise<StreamMessage | AppendResult> {
    const meta = await this.getValidMetadata(path);
    if (!meta) {
      throw new Error(`Stream not found: ${path}`);
    }

    const closedResult = this.handleClosedAppend(meta, options);
    if (closedResult) {
      return closedResult;
    }

    validateAppendContentType(meta.contentType, options?.contentType);
    validateAppendSeq(meta.lastSeq, options?.seq);

    const message = await this.appendData(path, meta, data);

    if (options?.seq !== undefined) {
      meta.lastSeq = options.seq;
    }

    if (options?.close) {
      this.markClosed(meta, options);
    }

    await this.saveMeta(path, meta);
    this.updateCache(path, meta);
    this.notifyWaiters(path);

    return message;
  }

  async appendWithProducer(
    path: string,
    data: Uint8Array,
    options: AppendOptions
  ): Promise<AppendResult> {
    if (!options.producerId) {
      const result = await this.append(path, data, options);
      return "message" in result ? result : { message: result };
    }

    const release = await this.acquirePathLock(path);
    try {
      return await this.appendWithProducerValidation(path, data, options);
    } finally {
      release();
    }
  }

  async closeStream(
    path: string
  ): Promise<{ finalOffset: string; alreadyClosed: boolean } | null> {
    const release = await this.acquirePathLock(path);
    try {
      const meta = await this.getValidMetadata(path);
      if (!meta) {
        return null;
      }

      const alreadyClosed = meta.closed;
      meta.closed = true;

      await this.saveMeta(path, meta);
      this.updateCache(path, meta);
      this.notifyWaitersClosed(path);

      return { finalOffset: meta.nextOffset, alreadyClosed };
    } finally {
      release();
    }
  }

  async closeStreamWithProducer(
    path: string,
    options: { producerId: string; producerEpoch: number; producerSeq: number }
  ): Promise<{
    finalOffset: string;
    alreadyClosed: boolean;
    producerResult?: ProducerValidationResult;
  } | null> {
    const release = await this.acquirePathLock(path);
    try {
      return await this.closeStreamWithProducerLocked(path, options);
    } finally {
      release();
    }
  }

  async delete(path: string): Promise<boolean> {
    this.cancelWaiters(path);
    this.streamCache.delete(path);

    const meta = await this.kv.get(this.metaKey(path), "json");
    if (!meta) {
      return false;
    }

    await Promise.all([
      this.kv.delete(this.metaKey(path)),
      this.kv.delete(this.dataKey(path)),
      this.kv.delete(this.producersKey(path)),
    ]);

    return true;
  }

  async clear(): Promise<void> {
    for (const path of this.waiters.keys()) {
      this.cancelWaiters(path);
    }
    this.streamCache.clear();

    const keys = await this.kv.list({ prefix: "stream:" });
    await Promise.all(keys.keys.map((k) => this.kv.delete(k.name)));
  }

  cancelAllWaits(): void {
    for (const path of this.waiters.keys()) {
      this.cancelWaiters(path);
    }
  }

  async getCurrentOffset(path: string): Promise<string | undefined> {
    const meta = await this.getValidMetadata(path);
    return meta?.nextOffset;
  }

  async list(): Promise<string[]> {
    const result = await this.kv.list({ prefix: "stream:" });
    const paths = new Set<string>();

    for (const key of result.keys) {
      const match = STREAM_KEY_PATTERN.exec(key.name);
      if (match?.[1]) {
        paths.add(match[1]);
      }
    }

    return Array.from(paths);
  }

  // ---------------------------------------------------------------------------
  // Private helpers - append
  // ---------------------------------------------------------------------------

  private async appendData(
    path: string,
    meta: StreamRecord,
    data: Uint8Array
  ): Promise<StreamMessage> {
    const existingData = await this.getData(path);
    const isJson = isJsonContentType(meta.contentType);
    const newData = mergeData(existingData, data, isJson);

    meta.appendCount++;
    meta.nextOffset = formatOffset(meta.appendCount, newData.length);

    await this.kv.put(this.dataKey(path), newData);

    return {
      data: newData.slice(existingData.length),
      offset: meta.nextOffset,
      timestamp: Date.now(),
    };
  }

  private handleClosedAppend(
    meta: StreamRecord,
    options?: AppendOptions
  ): AppendResult | null {
    if (!meta.closed) {
      return null;
    }

    if (this.isDuplicateClosingRequest(meta, options)) {
      return {
        message: null,
        streamClosed: true,
        producerResult: {
          status: "duplicate",
          lastSeq: options?.producerSeq ?? 0,
        },
      };
    }

    return { message: null, streamClosed: true };
  }

  private isDuplicateClosingRequest(
    meta: StreamRecord,
    options?: AppendOptions
  ): boolean {
    if (!(options?.producerId && meta.closedBy)) {
      return false;
    }
    return (
      meta.closedBy.producerId === options.producerId &&
      meta.closedBy.epoch === options.producerEpoch &&
      meta.closedBy.seq === options.producerSeq
    );
  }

  private markClosed(meta: StreamRecord, options: AppendOptions): void {
    meta.closed = true;
    if (options.producerId !== undefined) {
      meta.closedBy = {
        producerId: options.producerId,
        epoch: options.producerEpoch ?? 0,
        seq: options.producerSeq ?? 0,
      };
    }
  }

  // ---------------------------------------------------------------------------
  // Private helpers - producer-validated append
  // ---------------------------------------------------------------------------

  private async appendWithProducerValidation(
    path: string,
    data: Uint8Array,
    options: AppendOptions
  ): Promise<AppendResult> {
    const meta = await this.getValidMetadata(path);
    if (!meta) {
      throw new Error(`Stream not found: ${path}`);
    }

    const closedResult = this.handleClosedAppend(meta, options);
    if (closedResult) {
      return closedResult;
    }

    const producerId = options.producerId;
    const producerEpoch = options.producerEpoch;
    const producerSeq = options.producerSeq;
    if (
      producerId === undefined ||
      producerEpoch === undefined ||
      producerSeq === undefined
    ) {
      throw new Error("Producer options are required for validated append");
    }

    const producers = await this.getProducers(path);

    const producerResult = this.validateProducer(
      producers,
      producerId,
      producerEpoch,
      producerSeq
    );

    if (producerResult.status !== "accepted") {
      return { message: null, producerResult };
    }

    validateAppendContentType(meta.contentType, options.contentType);
    validateAppendSeq(meta.lastSeq, options.seq);

    const message = await this.appendData(path, meta, data);
    this.commitProducerState(producers, producerResult);

    if (options.seq !== undefined) {
      meta.lastSeq = options.seq;
    }

    if (options.close) {
      this.markClosed(meta, options);
    }

    await Promise.all([
      this.saveMeta(path, meta),
      this.saveProducers(path, producers),
    ]);

    this.updateCache(path, meta);

    if (options.close) {
      this.notifyWaitersClosed(path);
    } else {
      this.notifyWaiters(path);
    }

    return {
      message,
      producerResult,
      streamClosed: options.close,
    };
  }

  // ---------------------------------------------------------------------------
  // Private helpers - close with producer
  // ---------------------------------------------------------------------------

  private async closeStreamWithProducerLocked(
    path: string,
    options: { producerId: string; producerEpoch: number; producerSeq: number }
  ): Promise<{
    finalOffset: string;
    alreadyClosed: boolean;
    producerResult?: ProducerValidationResult;
  } | null> {
    const meta = await this.getValidMetadata(path);
    if (!meta) {
      return null;
    }

    if (meta.closed) {
      return this.handleAlreadyClosedWithProducer(meta, options);
    }

    const producers = await this.getProducers(path);

    const producerResult = this.validateProducer(
      producers,
      options.producerId,
      options.producerEpoch,
      options.producerSeq
    );

    if (producerResult.status !== "accepted") {
      return {
        finalOffset: meta.nextOffset,
        alreadyClosed: false,
        producerResult,
      };
    }

    this.commitProducerState(producers, producerResult);
    meta.closed = true;
    meta.closedBy = {
      producerId: options.producerId,
      epoch: options.producerEpoch,
      seq: options.producerSeq,
    };

    await Promise.all([
      this.saveMeta(path, meta),
      this.saveProducers(path, producers),
    ]);

    this.updateCache(path, meta);
    this.notifyWaitersClosed(path);

    return {
      finalOffset: meta.nextOffset,
      alreadyClosed: false,
      producerResult,
    };
  }

  private handleAlreadyClosedWithProducer(
    meta: StreamRecord,
    options: { producerId: string; producerEpoch: number; producerSeq: number }
  ): {
    finalOffset: string;
    alreadyClosed: boolean;
    producerResult: ProducerValidationResult;
  } {
    if (
      meta.closedBy &&
      meta.closedBy.producerId === options.producerId &&
      meta.closedBy.epoch === options.producerEpoch &&
      meta.closedBy.seq === options.producerSeq
    ) {
      return {
        finalOffset: meta.nextOffset,
        alreadyClosed: true,
        producerResult: { status: "duplicate", lastSeq: options.producerSeq },
      };
    }

    return {
      finalOffset: meta.nextOffset,
      alreadyClosed: true,
      producerResult: { status: "stream_closed" },
    };
  }

  // ---------------------------------------------------------------------------
  // Producer validation
  // ---------------------------------------------------------------------------

  private validateProducer(
    producers: Map<string, ProducerState>,
    producerId: string,
    epoch: number,
    seq: number
  ): ProducerValidationResult {
    this.cleanupExpiredProducers(producers);

    const state = producers.get(producerId);
    const now = Date.now();

    if (!state) {
      return this.validateNewProducer(producerId, epoch, seq, now);
    }

    return this.validateExistingProducer(state, producerId, epoch, seq, now);
  }

  private validateNewProducer(
    producerId: string,
    epoch: number,
    seq: number,
    now: number
  ): ProducerValidationResult {
    if (seq !== 0) {
      return { status: "sequence_gap", expectedSeq: 0, receivedSeq: seq };
    }
    return {
      status: "accepted",
      isNew: true,
      producerId,
      proposedState: { epoch, lastSeq: 0, lastUpdated: now },
    };
  }

  private validateExistingProducer(
    state: ProducerState,
    producerId: string,
    epoch: number,
    seq: number,
    now: number
  ): ProducerValidationResult {
    if (epoch < state.epoch) {
      return { status: "stale_epoch", currentEpoch: state.epoch };
    }

    if (epoch > state.epoch) {
      if (seq !== 0) {
        return { status: "invalid_epoch_seq" };
      }
      return {
        status: "accepted",
        isNew: true,
        producerId,
        proposedState: { epoch, lastSeq: 0, lastUpdated: now },
      };
    }

    if (seq <= state.lastSeq) {
      return { status: "duplicate", lastSeq: state.lastSeq };
    }

    if (seq === state.lastSeq + 1) {
      return {
        status: "accepted",
        isNew: false,
        producerId,
        proposedState: { epoch, lastSeq: seq, lastUpdated: now },
      };
    }

    return {
      status: "sequence_gap",
      expectedSeq: state.lastSeq + 1,
      receivedSeq: seq,
    };
  }

  private commitProducerState(
    producers: Map<string, ProducerState>,
    result: ProducerValidationResult
  ): void {
    if (result.status !== "accepted") {
      return;
    }
    producers.set(result.producerId, result.proposedState);
  }

  private cleanupExpiredProducers(producers: Map<string, ProducerState>): void {
    const now = Date.now();
    for (const [id, state] of producers) {
      if (now - state.lastUpdated > PRODUCER_STATE_TTL_MS) {
        producers.delete(id);
      }
    }
  }

  // ---------------------------------------------------------------------------
  // Path and producer locks
  // ---------------------------------------------------------------------------

  private async acquirePathLock(path: string): Promise<() => void> {
    while (this.pathLocks.has(path)) {
      await this.pathLocks.get(path);
    }

    let releaseLock: () => void;
    const lockPromise = new Promise<void>((resolve) => {
      releaseLock = resolve;
    });
    this.pathLocks.set(path, lockPromise);

    return () => {
      this.pathLocks.delete(path);
      releaseLock?.();
    };
  }

  // ---------------------------------------------------------------------------
  // Waiter management
  // ---------------------------------------------------------------------------

  private async notifyWaiters(path: string): Promise<void> {
    const pathWaiters = this.waiters.get(path);
    if (!pathWaiters || pathWaiters.length === 0) {
      return;
    }

    const meta = await this.getMetadata(path);
    if (!meta) {
      return;
    }

    const data = await this.getData(path);
    const remaining: Waiter[] = [];

    for (const waiter of pathWaiters) {
      const byteOffset = offsetToBytePos(waiter.offset);
      if (byteOffset < data.length) {
        clearTimeout(waiter.timer);
        waiter.resolve({
          messages: [
            {
              offset: meta.nextOffset,
              timestamp: Date.now(),
              data: data.slice(byteOffset),
            },
          ],
          timedOut: false,
        });
      } else {
        remaining.push(waiter);
      }
    }

    this.waiters.set(path, remaining);
  }

  private notifyWaitersClosed(path: string): void {
    const pathWaiters = this.waiters.get(path);
    if (!pathWaiters) {
      return;
    }

    for (const waiter of pathWaiters) {
      clearTimeout(waiter.timer);
      waiter.resolve({ messages: [], timedOut: false, streamClosed: true });
    }

    this.waiters.set(path, []);
  }

  private cancelWaiters(path: string): void {
    const pathWaiters = this.waiters.get(path);
    if (!pathWaiters) {
      return;
    }

    for (const waiter of pathWaiters) {
      clearTimeout(waiter.timer);
      waiter.resolve({ messages: [], timedOut: false });
    }

    this.waiters.delete(path);
  }

  private removeWaiter(path: string, waiter: Waiter): void {
    const pathWaiters = this.waiters.get(path);
    if (!pathWaiters) {
      return;
    }

    const index = pathWaiters.indexOf(waiter);
    if (index !== -1) {
      pathWaiters.splice(index, 1);
    }
  }
}
