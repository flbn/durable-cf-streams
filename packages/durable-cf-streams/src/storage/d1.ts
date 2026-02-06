import type {
  AppendOptions,
  AppendResult,
  DurableStreamStore,
  ProducerState,
  ProducerValidationResult,
  Stream,
  StreamMessage,
} from "@durable-streams/server/handler";
import { formatOffset, initialOffset, offsetToBytePos } from "../offsets.js";
import {
  formatJsonResponse,
  isJsonContentType,
  isMetadataExpired,
} from "../protocol.js";
import { PayloadTooLargeError } from "../errors.js";
import {
  mergeData,
  prepareInitialData,
  validateAppendContentType,
  validateAppendSeq,
  validateIdempotentCreate,
} from "./utils.js";

const PRODUCER_STATE_TTL_MS = 7 * 24 * 60 * 60 * 1000;

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
  closed: number;
  closed_by: string | null;
};

type ProducerRow = {
  path: string;
  producer_id: string;
  epoch: number;
  last_seq: number;
  last_updated: number;
};

type Waiter = {
  offset: string;
  resolve: (result: {
    messages: Array<StreamMessage>;
    timedOut: boolean;
    streamClosed?: boolean;
  }) => void;
  timer: ReturnType<typeof setTimeout>;
};

const isRowExpired = (row: {
  ttl_seconds: number | null;
  expires_at: string | null;
  created_at: number;
}): boolean =>
  isMetadataExpired({
    ttlSeconds: row.ttl_seconds ?? undefined,
    expiresAt: row.expires_at ?? undefined,
    createdAt: row.created_at,
  });

export class D1Store implements DurableStreamStore {
  private readonly db: D1Database;
  private readonly waiters = new Map<string, Waiter[]>();
  private readonly streamCache = new Map<
    string,
    {
      contentType: string;
      closed: boolean;
      nextOffset: string;
      lastSeq?: string;
      ttlSeconds?: number;
      expiresAt?: string;
      createdAt: number;
    }
  >();
  private readonly pathLocks = new Map<string, Promise<unknown>>();
  private readonly producerLocks = new Map<string, Promise<unknown>>();

  constructor(db: D1Database) {
    this.db = db;
  }

  static schema = [
    `CREATE TABLE IF NOT EXISTS streams (
      path TEXT PRIMARY KEY,
      content_type TEXT NOT NULL,
      ttl_seconds INTEGER,
      expires_at TEXT,
      created_at INTEGER NOT NULL,
      data BLOB NOT NULL DEFAULT x'',
      next_offset TEXT NOT NULL,
      last_seq TEXT,
      append_count INTEGER NOT NULL DEFAULT 0,
      closed INTEGER NOT NULL DEFAULT 0,
      closed_by TEXT
    )`,
    `CREATE TABLE IF NOT EXISTS producers (
      path TEXT NOT NULL,
      producer_id TEXT NOT NULL,
      epoch INTEGER NOT NULL,
      last_seq INTEGER NOT NULL,
      last_updated INTEGER NOT NULL,
      PRIMARY KEY (path, producer_id)
    )`,
  ] as const;

  async initialize(): Promise<void> {
    await this.db.batch(
      D1Store.schema.map((stmt) => this.db.prepare(stmt))
    );
  }

  async has(path: string): Promise<boolean> {
    const cached = this.streamCache.get(path);
    if (cached) {
      if (isMetadataExpired(cached)) {
        this.streamCache.delete(path);
        return false;
      }
      return true;
    }
    const row = await this.fetchStreamMetadata(path);
    if (!row || isRowExpired(row)) {
      this.streamCache.delete(path);
      return false;
    }
    this.updateCache(path, row);
    return true;
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
    const existing = await this.fetchStreamMetadata(path);

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
      this.updateCache(path, existing);
      return this.buildStreamFromRow(existing);
    }

    if (existing) {
      await this.deleteRow(path);
    }

    return this.insertNewStream(path, options);
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
  ): Promise<{ messages: Array<StreamMessage>; upToDate: boolean }> {
    const row = await this.fetchStreamWithData(path);
    if (!row || isRowExpired(row)) {
      throw new Error(`Stream not found: ${path}`);
    }

    const startOffset = offset ?? initialOffset();
    const byteOffset = offsetToBytePos(startOffset);
    const data = new Uint8Array(row.data);
    const messages: Array<StreamMessage> = [];

    if (byteOffset < data.length) {
      messages.push({
        offset: row.next_offset,
        timestamp: Date.now(),
        data: data.slice(byteOffset),
      });
    }

    return { messages, upToDate: true };
  }

  formatResponse(path: string, messages: Array<StreamMessage>): Uint8Array {
    const cached = this.streamCache.get(path);
    if (!cached) return new Uint8Array(0);

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

  async waitForMessages(
    path: string,
    offset: string,
    timeoutMs: number
  ): Promise<{
    messages: Array<StreamMessage>;
    timedOut: boolean;
    streamClosed?: boolean;
  }> {
    const row = await this.fetchStreamWithData(path);
    if (!row || isRowExpired(row)) {
      throw new Error(`Stream not found: ${path}`);
    }

    const immediateResult = this.checkImmediateData(row, offset);
    if (immediateResult) return immediateResult;

    if (row.closed === 1) {
      return { messages: [], timedOut: false, streamClosed: true };
    }

    return this.createWaiter(path, offset, timeoutMs);
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
    const row = await this.fetchStreamWithData(path);
    if (!row || isRowExpired(row)) {
      throw new Error(`Stream not found: ${path}`);
    }

    const closedResult = this.handleClosedAppend(row, options);
    if (closedResult) return closedResult;

    validateAppendContentType(row.content_type, options?.contentType);
    validateAppendSeq(row.last_seq ?? undefined, options?.seq);

    const message = await this.appendDataToDb(row, path, data, options);

    if (options?.close) {
      await this.markClosedInDb(path, options);
    }

    const updatedRow = await this.fetchStreamMetadata(path);
    if (updatedRow) this.updateCache(path, updatedRow);
    this.notifyWaiters(path, message);
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
      const row = await this.fetchStreamMetadata(path);
      if (!row || isRowExpired(row)) return null;

      const alreadyClosed = row.closed === 1;

      if (!alreadyClosed) {
        await this.db
          .prepare("UPDATE streams SET closed = 1 WHERE path = ?")
          .bind(path)
          .run();
      }

      this.updateCache(path, { ...row, closed: 1 });
      this.notifyWaitersClosed(path);

      return { finalOffset: row.next_offset, alreadyClosed };
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
    const pathWaiters = this.waiters.get(path);
    if (pathWaiters) {
      this.cancelWaitersForPath(pathWaiters);
    }
    this.waiters.delete(path);
    this.streamCache.delete(path);

    await this.db.batch([
      this.db.prepare("DELETE FROM producers WHERE path = ?").bind(path),
      this.db.prepare("DELETE FROM streams WHERE path = ?").bind(path),
    ]);

    return true;
  }

  async clear(): Promise<void> {
    for (const [, pathWaiters] of this.waiters) {
      this.cancelWaitersForPath(pathWaiters);
    }
    this.waiters.clear();
    this.streamCache.clear();

    await this.db.batch([
      this.db.prepare("DELETE FROM producers"),
      this.db.prepare("DELETE FROM streams"),
    ]);
  }

  cancelAllWaits(): void {
    for (const [, pathWaiters] of this.waiters) {
      this.cancelWaitersForPath(pathWaiters);
    }
    this.waiters.clear();
  }

  async getCurrentOffset(path: string): Promise<string | undefined> {
    const row = await this.db
      .prepare("SELECT next_offset FROM streams WHERE path = ?")
      .bind(path)
      .first<Pick<StreamRow, "next_offset">>();

    return row?.next_offset;
  }

  async list(): Promise<Array<string>> {
    const result = await this.db
      .prepare("SELECT path FROM streams")
      .all<Pick<StreamRow, "path">>();

    return result.results.map((r) => r.path);
  }

  // ---------------------------------------------------------------------------
  // DB helpers
  // ---------------------------------------------------------------------------

  private async fetchStreamMetadata(
    path: string
  ): Promise<Omit<StreamRow, "data"> | null> {
    const row = await this.db
      .prepare(
        "SELECT path, content_type, ttl_seconds, expires_at, created_at, next_offset, last_seq, append_count, closed, closed_by FROM streams WHERE path = ?"
      )
      .bind(path)
      .first<Omit<StreamRow, "data">>();

    return row ?? null;
  }

  private async fetchStreamWithData(path: string): Promise<StreamRow | null> {
    const row = await this.db
      .prepare(
        "SELECT path, content_type, ttl_seconds, expires_at, created_at, data, next_offset, last_seq, append_count, closed, closed_by FROM streams WHERE path = ?"
      )
      .bind(path)
      .first<StreamRow>();

    return row ?? null;
  }

  private async deleteRow(path: string): Promise<void> {
    await this.db.batch([
      this.db.prepare("DELETE FROM producers WHERE path = ?").bind(path),
      this.db.prepare("DELETE FROM streams WHERE path = ?").bind(path),
    ]);
  }

  private async insertNewStream(
    path: string,
    options: {
      contentType?: string;
      ttlSeconds?: number;
      expiresAt?: string;
      initialData?: Uint8Array;
      closed?: boolean;
    }
  ): Promise<Stream> {
    const contentType = options.contentType ?? "application/octet-stream";
    const { data, appendCount, nextOffset } = prepareInitialData({
      contentType,
      initialData: options.initialData,
    });
    const createdAt = Date.now();

    try {
      await this.db
        .prepare(
          `INSERT INTO streams (path, content_type, ttl_seconds, expires_at, created_at, data, next_offset, append_count, closed)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`
        )
        .bind(
          path,
          contentType,
          options.ttlSeconds ?? null,
          options.expiresAt ?? null,
          createdAt,
          data,
          nextOffset,
          appendCount,
          options.closed ? 1 : 0
        )
        .run();
    } catch (err) {
      const msg =
        err instanceof Error ? err.message.toLowerCase() : String(err);
      if (msg.includes("too big") || msg.includes("toobig") || msg.includes("d1_error")) {
        throw new PayloadTooLargeError(0, data.length);
      }
      throw err;
    }

    this.streamCache.set(path, {
      contentType,
      closed: options.closed ?? false,
      nextOffset,
      lastSeq: undefined,
      ttlSeconds: options.ttlSeconds,
      expiresAt: options.expiresAt,
      createdAt,
    });

    return {
      path,
      contentType,
      messages: [],
      currentOffset: nextOffset,
      lastSeq: undefined,
      ttlSeconds: options.ttlSeconds,
      expiresAt: options.expiresAt,
      createdAt,
      producers: new Map(),
      closed: options.closed || undefined,
    };
  }

  private buildStreamFromRow(row: Omit<StreamRow, "data">): Stream {
    return {
      path: row.path,
      contentType: row.content_type,
      messages: [],
      currentOffset: row.next_offset,
      lastSeq: row.last_seq ?? undefined,
      ttlSeconds: row.ttl_seconds ?? undefined,
      expiresAt: row.expires_at ?? undefined,
      createdAt: row.created_at,
      closed: row.closed === 1 || undefined,
      closedBy: row.closed_by ? JSON.parse(row.closed_by) : undefined,
    };
  }

  private updateCache(
    path: string,
    row: Omit<StreamRow, "data" | "path" | "append_count" | "closed_by">
  ): void {
    this.streamCache.set(path, {
      contentType: row.content_type,
      closed: row.closed === 1,
      nextOffset: row.next_offset,
      lastSeq: row.last_seq ?? undefined,
      ttlSeconds: row.ttl_seconds ?? undefined,
      expiresAt: row.expires_at ?? undefined,
      createdAt: row.created_at,
    });
  }

  // ---------------------------------------------------------------------------
  // Append helpers
  // ---------------------------------------------------------------------------

  private async appendDataToDb(
    row: StreamRow,
    path: string,
    data: Uint8Array,
    options?: AppendOptions
  ): Promise<StreamMessage> {
    const existingData = new Uint8Array(row.data);
    const isJson = isJsonContentType(row.content_type);
    const newData = mergeData(existingData, data, isJson);

    const newAppendCount = row.append_count + 1;
    const nextOffset = formatOffset(newAppendCount, newData.length);

    try {
      await this.db
        .prepare(
          `UPDATE streams SET data = ?, next_offset = ?, last_seq = ?, append_count = ? WHERE path = ?`
        )
        .bind(
          newData,
          nextOffset,
          options?.seq ?? row.last_seq,
          newAppendCount,
          path
        )
        .run();
    } catch (err) {
      const msg =
        err instanceof Error ? err.message.toLowerCase() : String(err);
      if (msg.includes("too big") || msg.includes("toobig") || msg.includes("d1_error")) {
        throw new PayloadTooLargeError(0, newData.length);
      }
      throw err;
    }

    return {
      data: newData.slice(existingData.length),
      offset: nextOffset,
      timestamp: Date.now(),
    };
  }

  private handleClosedAppend(
    row: StreamRow,
    options?: AppendOptions
  ): AppendResult | null {
    if (row.closed !== 1) return null;

    if (this.isDuplicateClosingRequest(row, options)) {
      return {
        message: null,
        streamClosed: true,
        producerResult: {
          status: "duplicate",
          lastSeq: options!.producerSeq!,
        },
      };
    }

    return { message: null, streamClosed: true };
  }

  private isDuplicateClosingRequest(
    row: StreamRow,
    options?: AppendOptions
  ): boolean {
    if (!options?.producerId || !row.closed_by) return false;
    const closedBy = JSON.parse(row.closed_by) as {
      producerId: string;
      epoch: number;
      seq: number;
    };
    return (
      closedBy.producerId === options.producerId &&
      closedBy.epoch === options.producerEpoch &&
      closedBy.seq === options.producerSeq
    );
  }

  private async markClosedInDb(
    path: string,
    options: AppendOptions
  ): Promise<void> {
    const closedBy = options.producerId
      ? JSON.stringify({
          producerId: options.producerId,
          epoch: options.producerEpoch,
          seq: options.producerSeq,
        })
      : null;

    await this.db
      .prepare("UPDATE streams SET closed = 1, closed_by = ? WHERE path = ?")
      .bind(closedBy, path)
      .run();

    this.notifyWaitersClosed(path);
  }

  private async appendWithProducerValidation(
    path: string,
    data: Uint8Array,
    options: AppendOptions
  ): Promise<AppendResult> {
    const row = await this.fetchStreamWithData(path);
    if (!row || isRowExpired(row)) throw new Error(`Stream not found: ${path}`);

    const closedResult = this.handleClosedAppend(row, options);
    if (closedResult) return closedResult;

    const producerResult = await this.validateProducer(
      path,
      options.producerId!,
      options.producerEpoch!,
      options.producerSeq!
    );

    if (producerResult.status !== "accepted") {
      return { message: null, producerResult };
    }

    validateAppendContentType(row.content_type, options.contentType);
    validateAppendSeq(row.last_seq ?? undefined, options.seq);

    const message = await this.appendDataToDb(row, path, data, options);
    await this.commitProducerState(path, producerResult);

    if (options.seq !== undefined) {
      await this.db
        .prepare("UPDATE streams SET last_seq = ? WHERE path = ?")
        .bind(options.seq, path)
        .run();
    }

    if (options.close) {
      await this.markClosedInDb(path, options);
    }

    const updatedRow = await this.fetchStreamMetadata(path);
    if (updatedRow) this.updateCache(path, updatedRow);

    if (options.close) {
      this.notifyWaitersClosed(path);
    } else {
      this.notifyWaiters(path, message);
    }

    return { message, producerResult, streamClosed: options.close };
  }

  // ---------------------------------------------------------------------------
  // Close with producer
  // ---------------------------------------------------------------------------

  private async closeStreamWithProducerLocked(
    path: string,
    options: { producerId: string; producerEpoch: number; producerSeq: number }
  ): Promise<{
    finalOffset: string;
    alreadyClosed: boolean;
    producerResult?: ProducerValidationResult;
  } | null> {
    const row = await this.fetchStreamMetadata(path);
    if (!row || isRowExpired(row)) return null;

    if (row.closed === 1) {
      return this.handleAlreadyClosedWithProducer(row, options);
    }

    const producerResult = await this.validateProducer(
      path,
      options.producerId,
      options.producerEpoch,
      options.producerSeq
    );

    if (producerResult.status !== "accepted") {
      return { finalOffset: row.next_offset, alreadyClosed: false, producerResult };
    }

    await this.commitProducerState(path, producerResult);

    const closedBy = JSON.stringify({
      producerId: options.producerId,
      epoch: options.producerEpoch,
      seq: options.producerSeq,
    });

    await this.db
      .prepare("UPDATE streams SET closed = 1, closed_by = ? WHERE path = ?")
      .bind(closedBy, path)
      .run();

    this.updateCache(path, { ...row, closed: 1 });
    this.notifyWaitersClosed(path);

    return { finalOffset: row.next_offset, alreadyClosed: false, producerResult };
  }

  private handleAlreadyClosedWithProducer(
    row: Omit<StreamRow, "data">,
    options: { producerId: string; producerEpoch: number; producerSeq: number }
  ): {
    finalOffset: string;
    alreadyClosed: boolean;
    producerResult: ProducerValidationResult;
  } {
    if (row.closed_by) {
      const closedBy = JSON.parse(row.closed_by) as {
        producerId: string;
        epoch: number;
        seq: number;
      };
      if (
        closedBy.producerId === options.producerId &&
        closedBy.epoch === options.producerEpoch &&
        closedBy.seq === options.producerSeq
      ) {
        return {
          finalOffset: row.next_offset,
          alreadyClosed: true,
          producerResult: { status: "duplicate", lastSeq: options.producerSeq },
        };
      }
    }

    return {
      finalOffset: row.next_offset,
      alreadyClosed: true,
      producerResult: { status: "stream_closed" },
    };
  }

  // ---------------------------------------------------------------------------
  // Producer validation
  // ---------------------------------------------------------------------------

  private async validateProducer(
    path: string,
    producerId: string,
    epoch: number,
    seq: number
  ): Promise<ProducerValidationResult> {
    await this.cleanupExpiredProducers(path);

    const state = await this.db
      .prepare(
        "SELECT epoch, last_seq, last_updated FROM producers WHERE path = ? AND producer_id = ?"
      )
      .bind(path, producerId)
      .first<Pick<ProducerRow, "epoch" | "last_seq" | "last_updated">>();

    const now = Date.now();

    if (!state) {
      return this.validateNewProducer(producerId, epoch, seq, now);
    }

    return this.validateExistingProducer(
      { epoch: state.epoch, lastSeq: state.last_seq, lastUpdated: state.last_updated },
      producerId,
      epoch,
      seq,
      now
    );
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
      if (seq !== 0) return { status: "invalid_epoch_seq" };
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

  private async commitProducerState(
    path: string,
    result: ProducerValidationResult
  ): Promise<void> {
    if (result.status !== "accepted") return;

    await this.db
      .prepare(
        `INSERT INTO producers (path, producer_id, epoch, last_seq, last_updated)
         VALUES (?, ?, ?, ?, ?)
         ON CONFLICT (path, producer_id) DO UPDATE SET epoch = ?, last_seq = ?, last_updated = ?`
      )
      .bind(
        path,
        result.producerId,
        result.proposedState.epoch,
        result.proposedState.lastSeq,
        result.proposedState.lastUpdated,
        result.proposedState.epoch,
        result.proposedState.lastSeq,
        result.proposedState.lastUpdated
      )
      .run();
  }

  private async cleanupExpiredProducers(path: string): Promise<void> {
    const cutoff = Date.now() - PRODUCER_STATE_TTL_MS;
    await this.db
      .prepare(
        "DELETE FROM producers WHERE path = ? AND last_updated < ?"
      )
      .bind(path, cutoff)
      .run();
  }

  // ---------------------------------------------------------------------------
  // Path locks
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
  // Producer locks
  // ---------------------------------------------------------------------------

  private async acquireProducerLock(
    path: string,
    producerId: string
  ): Promise<() => void> {
    const lockKey = `${path}:${producerId}`;

    while (this.producerLocks.has(lockKey)) {
      await this.producerLocks.get(lockKey);
    }

    let releaseLock: () => void;
    const lockPromise = new Promise<void>((resolve) => {
      releaseLock = resolve;
    });
    this.producerLocks.set(lockKey, lockPromise);

    return () => {
      this.producerLocks.delete(lockKey);
      releaseLock!();
    };
  }

  // ---------------------------------------------------------------------------
  // Waiter management
  // ---------------------------------------------------------------------------

  private checkImmediateData(
    row: StreamRow,
    offset: string
  ): {
    messages: Array<StreamMessage>;
    timedOut: boolean;
    streamClosed?: boolean;
  } | null {
    const byteOffset = offsetToBytePos(offset);
    const data = new Uint8Array(row.data);

    if (byteOffset < data.length) {
      return {
        messages: [{ offset: row.next_offset, timestamp: Date.now(), data: data.slice(byteOffset) }],
        timedOut: false,
      };
    }

    return null;
  }

  private createWaiter(
    path: string,
    offset: string,
    timeoutMs: number
  ): Promise<{
    messages: Array<StreamMessage>;
    timedOut: boolean;
    streamClosed?: boolean;
  }> {
    return new Promise((resolve) => {
      const timer = setTimeout(() => {
        this.removeWaiter(path, waiter);
        resolve({ messages: [], timedOut: true });
      }, timeoutMs);

      const waiter: Waiter = { offset, resolve, timer };
      const pathWaiters = this.waiters.get(path) ?? [];
      pathWaiters.push(waiter);
      this.waiters.set(path, pathWaiters);
    });
  }

  private async notifyWaiters(path: string, _message?: StreamMessage): Promise<void> {
    const pathWaiters = this.waiters.get(path);
    if (!pathWaiters || pathWaiters.length === 0) return;

    const row = await this.fetchStreamWithData(path);
    if (!row) return;

    const data = new Uint8Array(row.data);
    const remaining: Waiter[] = [];

    for (const waiter of pathWaiters) {
      const byteOffset = offsetToBytePos(waiter.offset);
      if (byteOffset < data.length) {
        clearTimeout(waiter.timer);
        waiter.resolve({
          messages: [
            {
              offset: row.next_offset,
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
    if (!pathWaiters || pathWaiters.length === 0) return;

    for (const waiter of pathWaiters) {
      clearTimeout(waiter.timer);
      waiter.resolve({ messages: [], timedOut: false, streamClosed: true });
    }
    this.waiters.set(path, []);
  }

  private cancelWaitersForPath(pathWaiters: Waiter[]): void {
    for (const waiter of pathWaiters) {
      clearTimeout(waiter.timer);
      waiter.resolve({ messages: [], timedOut: false });
    }
  }

  private removeWaiter(path: string, waiter: Waiter): void {
    const pathWaiters = this.waiters.get(path);
    if (!pathWaiters) return;
    const index = pathWaiters.indexOf(waiter);
    if (index !== -1) {
      pathWaiters.splice(index, 1);
    }
  }
}
