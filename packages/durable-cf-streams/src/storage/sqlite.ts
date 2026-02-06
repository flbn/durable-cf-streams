import type {
  AppendOptions,
  AppendResult,
  DurableStreamStore,
  ProducerState,
  ProducerValidationResult,
  Stream,
  StreamMessage,
} from "@durable-streams/server/types";
import { PayloadTooLargeError } from "../errors.js";
import { formatOffset, initialOffset, offsetToBytePos } from "../offsets.js";
import {
  formatJsonResponse,
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
    messages: StreamMessage[];
    timedOut: boolean;
    streamClosed?: boolean;
  }) => void;
  timer: ReturnType<typeof setTimeout>;
};

type ClosedBy = {
  producerId: string;
  epoch: number;
  seq: number;
};

const STREAMS_SCHEMA = `
  CREATE TABLE IF NOT EXISTS streams (
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
  )
`;

const PRODUCERS_SCHEMA = `
  CREATE TABLE IF NOT EXISTS producers (
    path TEXT NOT NULL,
    producer_id TEXT NOT NULL,
    epoch INTEGER NOT NULL,
    last_seq INTEGER NOT NULL,
    last_updated INTEGER NOT NULL,
    PRIMARY KEY (path, producer_id)
  )
`;

function parseClosedBy(raw: string | null): ClosedBy | undefined {
  if (!raw) {
    return undefined;
  }
  try {
    return JSON.parse(raw) as ClosedBy;
  } catch {
    return undefined;
  }
}

function isRowExpired(row: StreamRow): boolean {
  return isMetadataExpired({
    ttlSeconds: row.ttl_seconds ?? undefined,
    expiresAt: row.expires_at ?? undefined,
    createdAt: row.created_at,
  });
}

type CachedStream = {
  contentType: string;
  closed: boolean;
  nextOffset: string;
  lastSeq?: string;
  ttlSeconds?: number;
  expiresAt?: string;
  createdAt: number;
};

export class SqliteStore implements DurableStreamStore {
  private readonly sql: SqlStorage;
  private readonly waiters = new Map<string, Waiter[]>();
  private readonly streamCache = new Map<string, CachedStream>();
  private readonly pathLocks = new Map<string, Promise<unknown>>();

  static schema = [STREAMS_SCHEMA, PRODUCERS_SCHEMA];

  constructor(sql: SqlStorage) {
    this.sql = sql;
  }

  initialize(): void {
    this.sql.exec(STREAMS_SCHEMA);
    this.sql.exec(PRODUCERS_SCHEMA);
  }

  // ---------------------------------------------------------------------------
  // Row helpers
  // ---------------------------------------------------------------------------

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
      this.sql.exec("DELETE FROM producers WHERE path = ?", path);
      this.streamCache.delete(path);
      return null;
    }

    this.streamCache.set(path, {
      contentType: row.content_type,
      closed: row.closed === 1,
      nextOffset: row.next_offset,
      lastSeq: row.last_seq ?? undefined,
      ttlSeconds: row.ttl_seconds ?? undefined,
      expiresAt: row.expires_at ?? undefined,
      createdAt: row.created_at,
    });
    return row;
  }

  private buildStream(row: StreamRow): Stream {
    const producers = this.loadProducers(row.path);
    return {
      path: row.path,
      contentType: row.content_type,
      messages: [],
      currentOffset: row.next_offset,
      lastSeq: row.last_seq ?? undefined,
      ttlSeconds: row.ttl_seconds ?? undefined,
      expiresAt: row.expires_at ?? undefined,
      createdAt: row.created_at,
      producers,
      closed: row.closed === 1 || undefined,
      closedBy: parseClosedBy(row.closed_by),
    };
  }

  private loadProducers(path: string): Map<string, ProducerState> {
    const rows = this.sql
      .exec("SELECT * FROM producers WHERE path = ?", path)
      .toArray() as ProducerRow[];

    const map = new Map<string, ProducerState>();
    for (const row of rows) {
      map.set(row.producer_id, {
        epoch: row.epoch,
        lastSeq: row.last_seq,
        lastUpdated: row.last_updated,
      });
    }
    return map;
  }

  // ---------------------------------------------------------------------------
  // DurableStreamStore implementation
  // ---------------------------------------------------------------------------

  has(path: string): boolean {
    const cached = this.streamCache.get(path);
    if (cached) {
      if (isMetadataExpired(cached)) {
        this.streamCache.delete(path);
      } else {
        return true;
      }
    }
    return this.getStreamRow(path) !== null;
  }

  // biome-ignore lint/complexity/noExcessiveCognitiveComplexity: protocol requires these checks
  create(
    path: string,
    options: {
      contentType?: string;
      ttlSeconds?: number;
      expiresAt?: string;
      initialData?: Uint8Array;
      closed?: boolean;
    }
  ): Stream {
    const existing = this.getStreamRow(path);

    if (existing) {
      validateIdempotentCreate(
        {
          contentType: existing.content_type,
          ttlSeconds: existing.ttl_seconds ?? undefined,
          expiresAt: existing.expires_at ?? undefined,
          closed: existing.closed === 1,
        },
        options
      );
      return this.buildStream(existing);
    }

    const contentType = options.contentType ?? "application/octet-stream";
    const { data, appendCount, nextOffset } = prepareInitialData({
      contentType,
      initialData: options.initialData,
    });

    try {
      this.sql.exec(
        `INSERT INTO streams (path, content_type, ttl_seconds, expires_at, created_at, data, next_offset, append_count, closed)
         VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`,
        path,
        contentType,
        options.ttlSeconds ?? null,
        options.expiresAt ?? null,
        Date.now(),
        data,
        nextOffset,
        appendCount,
        options.closed ? 1 : 0
      );
    } catch (err) {
      const msg =
        err instanceof Error ? err.message.toLowerCase() : String(err);
      if (
        msg.includes("too big") ||
        msg.includes("toobig") ||
        msg.includes("sqlite_error")
      ) {
        throw new PayloadTooLargeError(0, data.length);
      }
      throw err;
    }

    const row = this.getStreamRow(path);
    if (!row) {
      throw new Error(`Stream not found after creation: ${path}`);
    }
    return this.buildStream(row);
  }

  get(path: string): Stream | undefined {
    const cached = this.streamCache.get(path);
    if (!cached) {
      const row = this.getStreamRow(path);
      if (!row) {
        return undefined;
      }
      return this.buildStream(row);
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

  read(
    path: string,
    offset?: string
  ): { messages: StreamMessage[]; upToDate: boolean } {
    const row = this.getStreamRow(path);
    if (!row) {
      throw new Error(`Stream not found: ${path}`);
    }

    const startOffset = offset ?? initialOffset();
    const byteOffset = offsetToBytePos(startOffset);
    const data = new Uint8Array(row.data);
    const messages: StreamMessage[] = [];

    if (byteOffset < data.length) {
      messages.push({
        offset: row.next_offset,
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
    return isJson ? formatJsonResponse(combined) : combined;
  }

  waitForMessages(
    path: string,
    offset: string,
    timeoutMs: number
  ): Promise<{
    messages: StreamMessage[];
    timedOut: boolean;
    streamClosed?: boolean;
  }> {
    const row = this.getStreamRow(path);
    if (!row) {
      return Promise.reject(new Error(`Stream not found: ${path}`));
    }

    const data = new Uint8Array(row.data);
    const byteOffset = offsetToBytePos(offset);

    if (byteOffset < data.length) {
      return Promise.resolve({
        messages: [
          {
            offset: row.next_offset,
            timestamp: Date.now(),
            data: data.slice(byteOffset),
          },
        ],
        timedOut: false,
      });
    }

    if (row.closed === 1) {
      return Promise.resolve({
        messages: [],
        timedOut: false,
        streamClosed: true,
      });
    }

    return new Promise((resolve) => {
      const timer = setTimeout(() => {
        this.removeWaiter(path, waiter);
        const current = this.getStreamRow(path);
        resolve({
          messages: [],
          timedOut: true,
          streamClosed: current?.closed === 1 || undefined,
        });
      }, timeoutMs);

      const waiter: Waiter = { offset, resolve, timer };
      const list = this.waiters.get(path) ?? [];
      list.push(waiter);
      this.waiters.set(path, list);
    });
  }

  async append(
    path: string,
    data: Uint8Array,
    options?: AppendOptions
  ): Promise<StreamMessage | AppendResult> {
    const release = await this.acquirePathLock(path);
    try {
      return this.appendLocked(path, data, options);
    } finally {
      release();
    }
  }

  private appendLocked(
    path: string,
    data: Uint8Array,
    options?: AppendOptions
  ): StreamMessage | AppendResult {
    const row = this.getStreamRow(path);
    if (!row) {
      throw new Error(`Stream not found: ${path}`);
    }

    const closedResult = this.handleClosedAppend(row, options);
    if (closedResult) {
      return closedResult;
    }

    validateAppendContentType(row.content_type, options?.contentType);
    validateAppendSeq(row.last_seq ?? undefined, options?.seq);

    const message = this.appendData(row, data, options);

    if (options?.close) {
      this.markClosed(row.path, options);
    }

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
      return this.appendWithProducerValidation(path, data, options);
    } finally {
      release();
    }
  }

  async closeStream(
    path: string
  ): Promise<{ finalOffset: string; alreadyClosed: boolean } | null> {
    const release = await this.acquirePathLock(path);
    try {
      const row = this.getStreamRow(path);
      if (!row) {
        return null;
      }

      const alreadyClosed = row.closed === 1;

      this.sql.exec("UPDATE streams SET closed = 1 WHERE path = ?", path);

      this.updateCache(path, { closed: true });
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
      return this.closeStreamWithProducerLocked(path, options);
    } finally {
      release();
    }
  }

  delete(path: string): boolean {
    const row = this.getStreamRow(path);
    if (!row) {
      return false;
    }

    this.cancelWaitersForPath(path);
    this.streamCache.delete(path);
    this.sql.exec("DELETE FROM streams WHERE path = ?", path);
    this.sql.exec("DELETE FROM producers WHERE path = ?", path);
    return true;
  }

  clear(): void {
    for (const path of this.waiters.keys()) {
      this.cancelWaitersForPath(path);
    }
    this.streamCache.clear();
    this.sql.exec("DELETE FROM streams");
    this.sql.exec("DELETE FROM producers");
  }

  cancelAllWaits(): void {
    for (const path of this.waiters.keys()) {
      this.cancelWaitersForPath(path);
    }
  }

  getCurrentOffset(path: string): string | undefined {
    const row = this.getStreamRow(path);
    return row?.next_offset;
  }

  list(): string[] {
    const rows = this.sql.exec("SELECT path FROM streams").toArray() as Array<{
      path: string;
    }>;
    return rows.map((r) => r.path);
  }

  private updateCache(path: string, updates: Partial<CachedStream>): void {
    const cached = this.streamCache.get(path);
    if (cached) {
      this.streamCache.set(path, { ...cached, ...updates });
    }
  }

  // ---------------------------------------------------------------------------
  // Append helpers
  // ---------------------------------------------------------------------------

  private appendData(
    row: StreamRow,
    data: Uint8Array,
    options?: AppendOptions
  ): StreamMessage {
    const isJson = isJsonContentType(row.content_type);
    const existingData = new Uint8Array(row.data);
    const newData = mergeData(existingData, data, isJson);

    const newAppendCount = row.append_count + 1;
    const nextOffset = formatOffset(newAppendCount, newData.length);

    const lastSeq = options?.seq ?? row.last_seq ?? undefined;

    try {
      this.sql.exec(
        "UPDATE streams SET data = ?, next_offset = ?, append_count = ?, last_seq = ? WHERE path = ?",
        newData,
        nextOffset,
        newAppendCount,
        lastSeq ?? null,
        row.path
      );
    } catch (err) {
      const msg =
        err instanceof Error ? err.message.toLowerCase() : String(err);
      if (
        msg.includes("too big") ||
        msg.includes("toobig") ||
        msg.includes("sqlite_error")
      ) {
        throw new PayloadTooLargeError(0, newData.length);
      }
      throw err;
    }

    this.updateCache(row.path, { nextOffset, lastSeq: lastSeq ?? undefined });

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
    if (row.closed !== 1) {
      return null;
    }

    if (this.isDuplicateClosingRequest(row, options)) {
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
    row: StreamRow,
    options?: AppendOptions
  ): boolean {
    if (!options?.producerId) {
      return false;
    }
    const closedBy = parseClosedBy(row.closed_by);
    if (!closedBy) {
      return false;
    }
    return (
      closedBy.producerId === options.producerId &&
      closedBy.epoch === options.producerEpoch &&
      closedBy.seq === options.producerSeq
    );
  }

  private markClosed(path: string, options: AppendOptions): void {
    const closedBy = options.producerId
      ? JSON.stringify({
          producerId: options.producerId,
          epoch: options.producerEpoch,
          seq: options.producerSeq,
        })
      : null;

    this.sql.exec(
      "UPDATE streams SET closed = 1, closed_by = ? WHERE path = ?",
      closedBy,
      path
    );

    this.updateCache(path, { closed: true });
    this.notifyWaitersClosed(path);
  }

  // ---------------------------------------------------------------------------
  // Producer-validated append
  // ---------------------------------------------------------------------------

  private appendWithProducerValidation(
    path: string,
    data: Uint8Array,
    options: AppendOptions
  ): AppendResult {
    const row = this.getStreamRow(path);
    if (!row) {
      throw new Error(`Stream not found: ${path}`);
    }

    const closedResult = this.handleClosedAppend(row, options);
    if (closedResult) {
      return closedResult;
    }

    const { producerId, producerEpoch, producerSeq } = options;
    if (!producerId || producerEpoch == null || producerSeq == null) {
      throw new Error("Producer fields are required for validated append");
    }

    const producerResult = this.validateProducer(
      row.path,
      producerId,
      producerEpoch,
      producerSeq
    );

    if (producerResult.status !== "accepted") {
      return { message: null, producerResult };
    }

    validateAppendContentType(row.content_type, options.contentType);
    validateAppendSeq(row.last_seq ?? undefined, options.seq);

    const message = this.appendData(row, data, options);
    this.commitProducerState(row.path, producerResult);

    if (options.close) {
      this.markClosed(path, options);
    }

    this.notifyWaiters(path);

    return { message, producerResult, streamClosed: options.close };
  }

  // ---------------------------------------------------------------------------
  // Close with producer
  // ---------------------------------------------------------------------------

  private closeStreamWithProducerLocked(
    path: string,
    options: { producerId: string; producerEpoch: number; producerSeq: number }
  ): {
    finalOffset: string;
    alreadyClosed: boolean;
    producerResult?: ProducerValidationResult;
  } | null {
    const row = this.getStreamRow(path);
    if (!row) {
      return null;
    }

    if (row.closed === 1) {
      return this.handleAlreadyClosedWithProducer(row, options);
    }

    const producerResult = this.validateProducer(
      path,
      options.producerId,
      options.producerEpoch,
      options.producerSeq
    );

    if (producerResult.status !== "accepted") {
      return {
        finalOffset: row.next_offset,
        alreadyClosed: false,
        producerResult,
      };
    }

    this.commitProducerState(path, producerResult);

    const closedBy = JSON.stringify({
      producerId: options.producerId,
      epoch: options.producerEpoch,
      seq: options.producerSeq,
    });

    this.sql.exec(
      "UPDATE streams SET closed = 1, closed_by = ? WHERE path = ?",
      closedBy,
      path
    );

    this.updateCache(path, { closed: true });
    this.notifyWaitersClosed(path);

    return {
      finalOffset: row.next_offset,
      alreadyClosed: false,
      producerResult,
    };
  }

  private handleAlreadyClosedWithProducer(
    row: StreamRow,
    options: { producerId: string; producerEpoch: number; producerSeq: number }
  ): {
    finalOffset: string;
    alreadyClosed: boolean;
    producerResult: ProducerValidationResult;
  } {
    const closedBy = parseClosedBy(row.closed_by);
    if (
      closedBy &&
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

    return {
      finalOffset: row.next_offset,
      alreadyClosed: true,
      producerResult: { status: "stream_closed" },
    };
  }

  // ---------------------------------------------------------------------------
  // Producer validation
  // ---------------------------------------------------------------------------

  private validateProducer(
    path: string,
    producerId: string,
    epoch: number,
    seq: number
  ): ProducerValidationResult {
    this.cleanupExpiredProducers(path);

    const rows = this.sql
      .exec(
        "SELECT * FROM producers WHERE path = ? AND producer_id = ?",
        path,
        producerId
      )
      .toArray() as ProducerRow[];

    const now = Date.now();

    const row = rows[0];
    if (!row) {
      return this.validateNewProducer(producerId, epoch, seq, now);
    }

    const state: ProducerState = {
      epoch: row.epoch,
      lastSeq: row.last_seq,
      lastUpdated: row.last_updated,
    };

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
    path: string,
    result: ProducerValidationResult
  ): void {
    if (result.status !== "accepted") {
      return;
    }

    this.sql.exec(
      `INSERT INTO producers (path, producer_id, epoch, last_seq, last_updated)
       VALUES (?, ?, ?, ?, ?)
       ON CONFLICT (path, producer_id) DO UPDATE SET
         epoch = excluded.epoch,
         last_seq = excluded.last_seq,
         last_updated = excluded.last_updated`,
      path,
      result.producerId,
      result.proposedState.epoch,
      result.proposedState.lastSeq,
      result.proposedState.lastUpdated
    );
  }

  private cleanupExpiredProducers(path: string): void {
    const cutoff = Date.now() - PRODUCER_STATE_TTL_MS;
    this.sql.exec(
      "DELETE FROM producers WHERE path = ? AND last_updated < ?",
      path,
      cutoff
    );
  }

  // ---------------------------------------------------------------------------
  // Path & producer locks
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

  private notifyWaiters(path: string): void {
    const row = this.getStreamRow(path);
    if (!row) {
      return;
    }

    const waiters = this.waiters.get(path) ?? [];
    this.waiters.set(path, []);

    const data = new Uint8Array(row.data);

    for (const waiter of waiters) {
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
        const remaining = this.waiters.get(path) ?? [];
        remaining.push(waiter);
        this.waiters.set(path, remaining);
      }
    }
  }

  private notifyWaitersClosed(path: string): void {
    const waiters = this.waiters.get(path) ?? [];
    this.waiters.set(path, []);

    for (const waiter of waiters) {
      clearTimeout(waiter.timer);
      waiter.resolve({ messages: [], timedOut: false, streamClosed: true });
    }
  }

  private cancelWaitersForPath(path: string): void {
    const waiters = this.waiters.get(path) ?? [];
    for (const waiter of waiters) {
      clearTimeout(waiter.timer);
      waiter.resolve({ messages: [], timedOut: false });
    }
    this.waiters.delete(path);
  }

  private removeWaiter(path: string, waiter: Waiter): void {
    const list = this.waiters.get(path);
    if (!list) {
      return;
    }
    const index = list.indexOf(waiter);
    if (index !== -1) {
      list.splice(index, 1);
    }
  }
}
