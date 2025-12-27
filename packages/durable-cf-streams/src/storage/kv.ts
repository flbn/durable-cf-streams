import { Deferred, Effect } from "effect";
import { calculateCursor } from "../cursor.js";
import {
  ContentTypeMismatchError,
  SequenceConflictError,
  StreamConflictError,
  StreamNotFoundError,
} from "../errors.js";
import { formatOffset, initialOffset, offsetToBytePos } from "../offsets.js";
import {
  formatJsonResponse,
  generateETag,
  isJsonContentType,
  normalizeContentType,
  processJsonAppend,
  validateJsonCreate,
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

type StreamRecord = {
  contentType: string;
  ttlSeconds?: number;
  expiresAt?: string;
  createdAt: number;
  nextOffset: string;
  lastSeq?: string;
  appendCount: number;
};

type Waiter = {
  deferred: Deferred.Deferred<WaitResult>;
  offset: Offset;
};

function validateIdempotentCreate(
  existing: { contentType: string; ttlSeconds?: number; expiresAt?: string },
  options: PutOptions
): void {
  const existingNormalized = normalizeContentType(existing.contentType);
  const reqNormalized = normalizeContentType(options.contentType);

  if (existingNormalized !== reqNormalized) {
    throw new ContentTypeMismatchError(existingNormalized, reqNormalized);
  }

  if (options.ttlSeconds !== existing.ttlSeconds) {
    throw new StreamConflictError("TTL mismatch on idempotent create");
  }

  if (options.expiresAt !== existing.expiresAt) {
    throw new StreamConflictError("Expires-At mismatch on idempotent create");
  }
}

function prepareInitialData(options: PutOptions): {
  data: Uint8Array;
  appendCount: number;
  nextOffset: string;
} {
  let data = options.data ?? new Uint8Array(0);
  const isJson = isJsonContentType(options.contentType);

  if (isJson && data.length > 0) {
    data = validateJsonCreate(data, true);
  }

  const appendCount = data.length > 0 ? 1 : 0;
  const nextOffset = formatOffset(appendCount, data.length);

  return { data, appendCount, nextOffset };
}

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

  async put(path: string, options: PutOptions): Promise<PutResult> {
    const existingMeta = await this.kv.get<StreamRecord>(
      this.metaKey(path),
      "json"
    );

    if (existingMeta) {
      validateIdempotentCreate(existingMeta, options);
      return { created: false, nextOffset: existingMeta.nextOffset };
    }

    const { data, appendCount, nextOffset } = prepareInitialData(options);

    const meta: StreamRecord = {
      contentType: options.contentType,
      ttlSeconds: options.ttlSeconds,
      expiresAt: options.expiresAt,
      createdAt: Date.now(),
      nextOffset,
      appendCount,
    };

    await Promise.all([
      this.kv.put(this.metaKey(path), JSON.stringify(meta)),
      this.kv.put(this.dataKey(path), data),
    ]);

    this.streamCache.set(path, { contentType: options.contentType });

    return { created: true, nextOffset };
  }

  async append(
    path: string,
    data: Uint8Array,
    options?: AppendOptions
  ): Promise<AppendResult> {
    const meta = await this.kv.get<StreamRecord>(this.metaKey(path), "json");

    if (!meta) {
      throw new StreamNotFoundError(path);
    }

    const streamNormalized = normalizeContentType(meta.contentType);
    if (options?.contentType) {
      const reqNormalized = normalizeContentType(options.contentType);
      if (streamNormalized !== reqNormalized) {
        throw new ContentTypeMismatchError(streamNormalized, reqNormalized);
      }
    }

    if (
      options?.seq !== undefined &&
      meta.lastSeq !== undefined &&
      options.seq <= meta.lastSeq
    ) {
      throw new SequenceConflictError(`> ${meta.lastSeq}`, options.seq);
    }

    const existingRaw = await this.kv.get(this.dataKey(path), "arrayBuffer");
    const existingData = existingRaw
      ? new Uint8Array(existingRaw)
      : new Uint8Array(0);

    const isJson = isJsonContentType(meta.contentType);
    let newData: Uint8Array;

    if (isJson) {
      newData = processJsonAppend(existingData, data);
    } else {
      newData = new Uint8Array(existingData.length + data.length);
      newData.set(existingData);
      newData.set(data, existingData.length);
    }

    const newAppendCount = meta.appendCount + 1;
    const nextOffset = formatOffset(newAppendCount, newData.length);

    const updatedMeta: StreamRecord = {
      ...meta,
      nextOffset,
      lastSeq: options?.seq ?? meta.lastSeq,
      appendCount: newAppendCount,
    };

    await Promise.all([
      this.kv.put(this.metaKey(path), JSON.stringify(updatedMeta)),
      this.kv.put(this.dataKey(path), newData),
    ]);

    this.notifyWaiters(path, newData);

    return { nextOffset };
  }

  async get(path: string, options?: GetOptions): Promise<GetResult> {
    const meta = await this.kv.get<StreamRecord>(this.metaKey(path), "json");

    if (!meta) {
      this.streamCache.delete(path);
      throw new StreamNotFoundError(path);
    }

    this.streamCache.set(path, { contentType: meta.contentType });

    const raw = await this.kv.get(this.dataKey(path), "arrayBuffer");
    const data = raw ? new Uint8Array(raw) : new Uint8Array(0);

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
    };
  }

  async head(path: string): Promise<HeadResult | null> {
    const meta = await this.kv.get<StreamRecord>(this.metaKey(path), "json");

    if (!meta) {
      this.streamCache.delete(path);
      return null;
    }

    this.streamCache.set(path, { contentType: meta.contentType });

    return {
      contentType: meta.contentType,
      nextOffset: meta.nextOffset,
      etag: generateETag(path, initialOffset(), meta.nextOffset),
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

    await Promise.all([
      this.kv.delete(this.metaKey(path)),
      this.kv.delete(this.dataKey(path)),
    ]);
  }

  has(path: string): boolean {
    return this.streamCache.has(path);
  }

  async waitForData(
    path: string,
    offset: Offset,
    timeoutMs: number
  ): Promise<WaitResult> {
    const meta = await this.kv.get<StreamRecord>(this.metaKey(path), "json");

    if (!meta) {
      throw new StreamNotFoundError(path);
    }

    const raw = await this.kv.get(this.dataKey(path), "arrayBuffer");
    const data = raw ? new Uint8Array(raw) : new Uint8Array(0);

    const byteOffset = offsetToBytePos(offset);

    if (byteOffset < data.length) {
      return {
        messages: [
          { offset, timestamp: Date.now(), data: data.slice(byteOffset) },
        ],
        timedOut: false,
      };
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
