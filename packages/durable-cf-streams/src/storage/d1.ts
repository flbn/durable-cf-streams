import { Deferred, Effect } from "effect";
import { calculateCursor } from "../cursor.js";
import {
  InvalidOffsetError,
  InvalidProducerError,
  PayloadTooLargeError,
  StreamClosedError,
  StreamConflictError,
  StreamNotFoundError,
} from "../errors.js";
import { formatOffset, initialOffset, offsetToBytePos } from "../offsets.js";
import {
  evaluateClaimedProducerAppend,
  evaluateProducerAppend,
  type ProducerAppendDecision,
} from "../producer.js";
import {
  formatJsonResponse,
  generateETag,
  isExpired,
  isJsonContentType,
  processJsonAppend,
} from "../protocol.js";
import type {
  AppendOptions,
  AppendResult,
  GetOptions,
  GetResult,
  HeadResult,
  Offset,
  ProducerClaim,
  ProducerState,
  PutOptions,
  PutResult,
  StreamIncarnation,
  StreamMessage,
  WaitOptions,
  WaitResult,
} from "../types.js";
import type { StreamStore } from "./interface.js";
import {
  CLOUDFLARE_SQL_MAX_VALUE_BYTES,
  isSqlPayloadTooLargeError,
  rethrowSqlPayloadTooLargeError,
} from "./platform-errors.js";
import {
  readSqlChunkMessages,
  type SqlChunkMessagesResult,
  type SqlChunkRow,
} from "./sql-chunks.js";
import {
  SQL_STREAMS_FORMAT_VERSION,
  SQL_STREAMS_META_SCHEMA,
} from "./sqlite-schema.js";
import {
  appendResult,
  assertStreamIncarnation,
  assertStreamLive,
  closedAppendResult,
  generateIncarnation,
  inheritedExpiration,
  normalizeForkSubOffset,
  prepareForkData,
  prepareInitialData,
  resolveCreateContentType,
  validateAppendContentType,
  validateAppendSeq,
  validateIdempotentCreate,
  validateReadOffset,
} from "./utils.js";
import { notifyDeletedWaiters, type Waiter } from "./waiters.js";

type StreamRow = {
  path: string;
  incarnation: StreamIncarnation;
  content_type: string;
  ttl_seconds: number | null;
  expires_at: string | null;
  created_at: number;
  last_accessed_at: number | null;
  next_offset: Offset;
  last_seq: string | null;
  producer_id: string | null;
  producer_epoch: number;
  next_producer_sequence: number;
  append_count: number;
  closed: number;
  forked_from: string | null;
  fork_offset: Offset | null;
  fork_sub_offset: number | null;
  child_count: number;
  deleted: number;
};

type ChunkRow = SqlChunkRow;

type ProducerRow = {
  epoch: number;
  seq: number;
};

type ProducerAppendRow = {
  start_offset: Offset;
  end_offset: Offset;
  data_length: number;
  closed: number;
};

type PreparedCreate = {
  readonly contentType: string;
  readonly incarnation: StreamIncarnation;
  readonly ttlSeconds?: number;
  readonly expiresAt?: string;
  readonly data: Uint8Array;
  readonly appendCount: number;
  readonly nextOffset: Offset;
  readonly closed: boolean;
  readonly forkedFrom?: string;
  readonly forkOffset?: Offset;
  readonly forkSubOffset?: number;
  readonly forkSource?: StreamRow;
};

type PreparedAppendChunk = {
  readonly data: Uint8Array;
  readonly appendCount: number;
  readonly nextOffset: Offset;
  readonly appended: boolean;
};

type ReadWindow = {
  readonly messages: StreamMessage[];
  readonly nextOffset: Offset;
  readonly upToDate: boolean;
};

type AppendCommitGuard = {
  readonly incarnation: StreamIncarnation;
  readonly nextOffset: Offset;
  readonly appendCount: number;
  readonly closed: number;
  readonly producerId: string | null;
  readonly producerEpoch: number;
  readonly nextProducerSequence: number;
};

type StreamMutationGuard = {
  readonly incarnation: StreamIncarnation;
  readonly nextOffset: Offset;
  readonly appendCount: number;
  readonly closed: number;
  readonly childCount: number;
  readonly deleted: number;
  readonly producerId: string | null;
  readonly producerEpoch: number;
  readonly nextProducerSequence: number;
};

type HardDeletePlanStep =
  | {
      readonly type: "delete";
      readonly path: string;
      readonly row: StreamRow;
      readonly releasedParent:
        | {
            readonly path: string;
            readonly row: StreamRow;
          }
        | undefined;
    }
  | {
      readonly type: "release";
      readonly path: string;
      readonly row: StreamRow;
      readonly childCount: number;
      readonly childPath: string;
      readonly childRow: StreamRow;
    };

const D1_STREAMS_SCHEMA =
  "CREATE TABLE IF NOT EXISTS streams (path TEXT PRIMARY KEY, incarnation TEXT NOT NULL, content_type TEXT NOT NULL, ttl_seconds INTEGER, expires_at TEXT, created_at INTEGER NOT NULL, last_accessed_at INTEGER, next_offset TEXT NOT NULL, last_seq TEXT, producer_id TEXT, producer_epoch INTEGER NOT NULL DEFAULT 0, next_producer_sequence INTEGER NOT NULL DEFAULT 0, append_count INTEGER NOT NULL DEFAULT 0, closed INTEGER NOT NULL DEFAULT 0, forked_from TEXT, fork_offset TEXT, fork_sub_offset INTEGER, child_count INTEGER NOT NULL DEFAULT 0, deleted INTEGER NOT NULL DEFAULT 0);";

/**
 * creates the stream metadata table used by `D1Store`.
 * NOTE: this is a breaking schema; unversioned SQL stores are rejected instead of migrated.
 */
const initializeD1StreamsSchema = async (db: D1Database): Promise<void> => {
  await db.exec(SQL_STREAMS_META_SCHEMA);
  const stored = await db
    .prepare("SELECT value FROM stream_meta WHERE key = 'format_version'")
    .first<{ value: string }>();
  if (stored !== null && String(stored.value) !== SQL_STREAMS_FORMAT_VERSION) {
    throw new Error(
      `Unsupported stream storage format ${String(stored.value)}; expected ${SQL_STREAMS_FORMAT_VERSION}`
    );
  }
  if (stored === null) {
    const existing = await db
      .prepare(
        "SELECT name FROM sqlite_master WHERE type = 'table' AND name IN ('streams', 'stream_chunks', 'stream_producers') LIMIT 1"
      )
      .first<{ name: string }>();
    if (existing !== null) {
      throw new Error(
        "Unsupported unversioned stream storage format; clear the old SQL stream tables before opening this breaking schema"
      );
    }
    await db
      .prepare(
        "INSERT INTO stream_meta (key, value) VALUES ('format_version', ?)"
      )
      .bind(SQL_STREAMS_FORMAT_VERSION)
      .run();
  }
  await db.exec(D1_STREAMS_SCHEMA);
};

export type D1StoreOptions = {
  /**
   * max bytes for one stored stream chunk.
   * NOTE: one append may spill into multiple chunk rows; each stored row stays below Cloudflare's SQL row and BLOB ceiling.
   */
  readonly maxChunkBytes?: number;
  /**
   * max stream bytes returned from one read.
   * NOTE: reads stop before the durable tail when the window fills; resume from the returned `nextOffset` until `upToDate` is true.
   */
  readonly maxReadBytes?: number;
  /**
   * max logical bytes accepted by one put or append before storage chunking.
   * NOTE: this bounds one API call; each stored chunk still stays under `maxChunkBytes`.
   * NOTE: `maxAppendBytes / maxChunkBytes` must stay near the default chunk count so one logical write cannot create an unbounded d1 batch.
   */
  readonly maxAppendBytes?: number;
};

export const DEFAULT_D1_MAX_CHUNK_BYTES = 512 * 1024;
export const DEFAULT_D1_MAX_READ_BYTES = DEFAULT_D1_MAX_CHUNK_BYTES;
/**
 * default ceiling for one put, fork result, or append before it reaches SQL.
 * NOTE: this mirrors the 12 MiB batch ceiling used upstream; larger tool or message output belongs upstream of persistence.
 */
export const DEFAULT_D1_MAX_APPEND_BYTES = 12 * 1024 * 1024;
const MAX_D1_CHUNKS_PER_WRITE = Math.ceil(
  DEFAULT_D1_MAX_APPEND_BYTES / DEFAULT_D1_MAX_CHUNK_BYTES
);

const isRowExpired = (row: {
  ttl_seconds: number | null;
  expires_at: string | null;
  created_at: number;
  last_accessed_at: number | null;
}): boolean =>
  isExpired({
    ttlSeconds: row.ttl_seconds ?? undefined,
    expiresAt: row.expires_at ?? undefined,
    createdAt: row.created_at,
    lastAccessedAt: row.last_accessed_at ?? undefined,
  });

const resolveMaxSqlBytes = (name: string, value: number): number => {
  if (
    !Number.isSafeInteger(value) ||
    value <= 0 ||
    value > CLOUDFLARE_SQL_MAX_VALUE_BYTES
  ) {
    throw new RangeError(
      `${name} must be an integer between 1 and ${CLOUDFLARE_SQL_MAX_VALUE_BYTES}`
    );
  }

  return value;
};

const resolveMaxLogicalBytes = (
  name: string,
  value: number,
  maxChunkBytes: number
): number => {
  if (!Number.isSafeInteger(value) || value <= 0) {
    throw new RangeError(`${name} must be a positive safe integer`);
  }
  if (Math.ceil(value / maxChunkBytes) > MAX_D1_CHUNKS_PER_WRITE) {
    throw new RangeError(
      `${name} would create more than ${MAX_D1_CHUNKS_PER_WRITE} SQL chunks per write`
    );
  }

  return value;
};

const bytesEqual = (left: Uint8Array, right: Uint8Array): boolean => {
  if (left.length !== right.length) {
    return false;
  }
  for (let index = 0; index < left.length; index += 1) {
    if (left[index] !== right[index]) {
      return false;
    }
  }
  return true;
};

const changedRows = (result: D1Result<unknown> | undefined): number =>
  typeof result?.meta?.changes === "number" ? result.meta.changes : 0;

const assertAppendCommitChangedOneRow = (
  path: string,
  result: D1Result<unknown> | undefined
): void => {
  if (changedRows(result) !== 1) {
    throw new StreamConflictError(
      `stream changed before append committed: ${path}`
    );
  }
};

const assertMutationChangedOneRow = (
  action: string,
  path: string,
  result: D1Result<unknown> | undefined
): void => {
  if (changedRows(result) !== 1) {
    throw new StreamConflictError(`stream changed before ${action}: ${path}`);
  }
};

const streamMutationGuard = (row: StreamRow): StreamMutationGuard => ({
  incarnation: row.incarnation,
  nextOffset: row.next_offset,
  appendCount: row.append_count,
  closed: row.closed,
  childCount: row.child_count,
  deleted: row.deleted,
  producerId: row.producer_id,
  producerEpoch: row.producer_epoch,
  nextProducerSequence: row.next_producer_sequence,
});

const guardedStreamWhere =
  "path = ? AND incarnation = ? AND next_offset = ? AND append_count = ? AND closed = ? AND child_count = ? AND deleted = ? AND producer_id IS ? AND producer_epoch = ? AND next_producer_sequence = ?";

const guardedAppendWhere =
  "path = ? AND incarnation = ? AND next_offset = ? AND append_count = ? AND closed = ? AND deleted = 0 AND producer_id IS ? AND producer_epoch = ? AND next_producer_sequence = ?";

const streamMutationGuardValues = (
  path: string,
  guard: StreamMutationGuard
): readonly unknown[] => [
  path,
  guard.incarnation,
  guard.nextOffset,
  guard.appendCount,
  guard.closed,
  guard.childCount,
  guard.deleted,
  guard.producerId,
  guard.producerEpoch,
  guard.nextProducerSequence,
];

const bindStreamMutationGuard = (
  statement: D1PreparedStatement,
  path: string,
  guard: StreamMutationGuard
): D1PreparedStatement =>
  statement.bind(...streamMutationGuardValues(path, guard));

const appendCommitGuard = (row: StreamRow): AppendCommitGuard => ({
  incarnation: row.incarnation,
  nextOffset: row.next_offset,
  appendCount: row.append_count,
  closed: row.closed,
  producerId: row.producer_id,
  producerEpoch: row.producer_epoch,
  nextProducerSequence: row.next_producer_sequence,
});

const appendCommitGuardValues = (
  path: string,
  guard: AppendCommitGuard
): readonly unknown[] => [
  path,
  guard.incarnation,
  guard.nextOffset,
  guard.appendCount,
  guard.closed,
  guard.producerId,
  guard.producerEpoch,
  guard.nextProducerSequence,
];

/**
 * d1 store backed by stream metadata rows, keyed producer state rows, and bounded append chunks.
 * NOTE: producer idempotency state lives in `stream_producers`, keeping the stream metadata row bounded.
 * NOTE: one append may spill into multiple bounded chunk rows while keeping one logical append index.
 * NOTE: write mutations compare the stream metadata row they read with the row they commit so misuse outside one serialized owner fails instead of racing silently.
 */
export class D1Store implements StreamStore {
  private readonly db: D1Database;
  private readonly maxChunkBytes: number;
  private readonly maxReadBytes: number;
  private readonly maxAppendBytes: number;
  private readonly waiters = new Map<string, Waiter[]>();
  private readonly streamCache = new Map<string, { contentType: string }>();

  private static chunkSchema =
    "CREATE TABLE IF NOT EXISTS stream_chunks (path TEXT NOT NULL, incarnation TEXT NOT NULL, append_index INTEGER NOT NULL, chunk_index INTEGER NOT NULL CHECK (chunk_index >= 0), chunk_count INTEGER NOT NULL CHECK (chunk_count > 0), start_pos INTEGER NOT NULL, end_pos INTEGER NOT NULL, start_offset TEXT NOT NULL, end_offset TEXT NOT NULL, data BLOB NOT NULL, PRIMARY KEY (path, incarnation, start_pos));";

  private static chunksByEndIndex =
    "CREATE INDEX IF NOT EXISTS stream_chunks_by_end ON stream_chunks(path, incarnation, end_pos);";

  private static chunksByAppendIndex =
    "CREATE INDEX IF NOT EXISTS stream_chunks_by_append ON stream_chunks(path, incarnation, append_index, chunk_index);";

  private static producerSchema =
    "CREATE TABLE IF NOT EXISTS stream_producers (path TEXT NOT NULL, incarnation TEXT NOT NULL, producer_id TEXT NOT NULL, epoch INTEGER NOT NULL, seq INTEGER NOT NULL, PRIMARY KEY (path, incarnation, producer_id));";

  private static producerAppendSchema =
    "CREATE TABLE IF NOT EXISTS stream_producer_appends (path TEXT NOT NULL, incarnation TEXT NOT NULL, producer_id TEXT NOT NULL, epoch INTEGER NOT NULL, seq INTEGER NOT NULL, append_index INTEGER NOT NULL, start_offset TEXT NOT NULL, end_offset TEXT NOT NULL, data_length INTEGER NOT NULL, closed INTEGER NOT NULL DEFAULT 0, PRIMARY KEY (path, incarnation, producer_id, epoch, seq));";

  static schema = `${SQL_STREAMS_META_SCHEMA}
${D1_STREAMS_SCHEMA}
${D1Store.chunkSchema}
${D1Store.chunksByEndIndex}
${D1Store.chunksByAppendIndex}
${D1Store.producerSchema}
${D1Store.producerAppendSchema}`;

  constructor(db: D1Database, options?: D1StoreOptions) {
    this.db = db;
    this.maxChunkBytes = resolveMaxSqlBytes(
      "maxChunkBytes",
      options?.maxChunkBytes ?? DEFAULT_D1_MAX_CHUNK_BYTES
    );
    this.maxReadBytes = resolveMaxSqlBytes(
      "maxReadBytes",
      options?.maxReadBytes ?? DEFAULT_D1_MAX_READ_BYTES
    );
    this.maxAppendBytes = resolveMaxLogicalBytes(
      "maxAppendBytes",
      options?.maxAppendBytes ?? DEFAULT_D1_MAX_APPEND_BYTES,
      this.maxChunkBytes
    );
  }

  async initialize(): Promise<void> {
    await initializeD1StreamsSchema(this.db);
    await this.db.exec(D1Store.chunkSchema);
    await this.db.exec(D1Store.chunksByEndIndex);
    await this.db.exec(D1Store.chunksByAppendIndex);
    await this.db.exec(D1Store.producerSchema);
    await this.db.exec(D1Store.producerAppendSchema);
  }

  /**
   * renews TTL without acting as a write fence.
   * NOTE: reads may race appends; a freshness touch that loses that race must not fail an otherwise valid read.
   */
  private async touchStream(path: string, row: StreamRow): Promise<StreamRow> {
    if (row.ttl_seconds === null) {
      return row;
    }

    const lastAccessedAt = Date.now();
    await this.db
      .prepare(
        "UPDATE streams SET last_accessed_at = ? WHERE path = ? AND incarnation = ? AND deleted = ?"
      )
      .bind(lastAccessedAt, path, row.incarnation, row.deleted)
      .run();
    return { ...row, last_accessed_at: lastAccessedAt };
  }

  private nextLastAccessedAt(row: StreamRow): number | null {
    return row.ttl_seconds === null ? row.last_accessed_at : Date.now();
  }

  private async expireStream(
    path: string,
    row: StreamRow
  ): Promise<StreamRow | null> {
    if (row.child_count > 0) {
      const result = await bindStreamMutationGuard(
        this.db.prepare(
          `UPDATE streams SET deleted = 1 WHERE ${guardedStreamWhere}`
        ),
        path,
        streamMutationGuard(row)
      ).run();
      assertMutationChangedOneRow("expire committed", path, result);
      this.notifyDeleted(path);
      return { ...row, deleted: 1 };
    }

    await this.hardDelete(path, row);
    return null;
  }

  private async hardDelete(path: string, row: StreamRow): Promise<void> {
    const plan = await this.collectHardDeletePlan(path, row, []);
    const statements = plan.flatMap((step) =>
      step.type === "delete"
        ? this.hardDeleteStatements(step.path, step.row, step.releasedParent)
        : [
            this.parentReleaseStatement(
              step.path,
              step.row,
              step.childCount,
              step.childPath,
              step.childRow
            ),
          ]
    );
    const results = await this.db.batch(statements);
    let resultIndex = 0;
    const deletedPaths: string[] = [];
    for (const step of plan) {
      if (step.type === "delete") {
        assertMutationChangedOneRow(
          "delete committed",
          step.path,
          results[resultIndex + 3]
        );
        deletedPaths.push(step.path);
        resultIndex += 4;
        continue;
      }

      assertMutationChangedOneRow(
        "parent release committed",
        step.path,
        results[resultIndex]
      );
      resultIndex += 1;
    }

    for (const deletedPath of deletedPaths) {
      this.notifyDeleted(deletedPath);
    }
  }

  private async collectHardDeletePlan(
    path: string,
    row: StreamRow,
    plan: HardDeletePlanStep[]
  ): Promise<HardDeletePlanStep[]> {
    let releasedParent:
      | {
          readonly path: string;
          readonly row: StreamRow;
        }
      | undefined;
    if (row.forked_from === null) {
      plan.push({ type: "delete", path, row, releasedParent });
      return plan;
    }

    const parent = await this.db
      .prepare("SELECT * FROM streams WHERE path = ?")
      .bind(row.forked_from)
      .first<StreamRow>();
    if (parent === null) {
      plan.push({ type: "delete", path, row, releasedParent });
      return plan;
    }

    const childCount = Math.max(0, parent.child_count - 1);
    releasedParent = {
      path: row.forked_from,
      row: { ...parent, child_count: childCount },
    };
    plan.push({
      type: "release",
      path: row.forked_from,
      row: parent,
      childCount,
      childPath: path,
      childRow: row,
    });
    plan.push({ type: "delete", path, row, releasedParent });
    if (parent.deleted === 1 && childCount === 0) {
      await this.collectHardDeletePlan(
        row.forked_from,
        releasedParent.row,
        plan
      );
    }

    return plan;
  }

  private hardDeleteStatements(
    path: string,
    row: StreamRow,
    releasedParent:
      | {
          readonly path: string;
          readonly row: StreamRow;
        }
      | undefined
  ): D1PreparedStatement[] {
    const guard = streamMutationGuard(row);
    const guardValues = streamMutationGuardValues(path, guard);
    const parentClause =
      releasedParent === undefined
        ? ""
        : ` AND EXISTS (SELECT 1 FROM streams WHERE ${guardedStreamWhere})`;
    const parentValues =
      releasedParent === undefined
        ? []
        : streamMutationGuardValues(
            releasedParent.path,
            streamMutationGuard(releasedParent.row)
          );
    return [
      this.db
        .prepare(
          `DELETE FROM stream_producers
           WHERE path = ? AND incarnation = ?
           AND EXISTS (SELECT 1 FROM streams WHERE ${guardedStreamWhere})${parentClause}`
        )
        .bind(path, row.incarnation, ...guardValues, ...parentValues),
      this.db
        .prepare(
          `DELETE FROM stream_producer_appends
           WHERE path = ? AND incarnation = ?
           AND EXISTS (SELECT 1 FROM streams WHERE ${guardedStreamWhere})${parentClause}`
        )
        .bind(path, row.incarnation, ...guardValues, ...parentValues),
      this.db
        .prepare(
          `DELETE FROM stream_chunks
           WHERE path = ? AND incarnation = ?
           AND EXISTS (SELECT 1 FROM streams WHERE ${guardedStreamWhere})${parentClause}`
        )
        .bind(path, row.incarnation, ...guardValues, ...parentValues),
      this.db
        .prepare(
          `DELETE FROM streams WHERE ${guardedStreamWhere}${parentClause}`
        )
        .bind(...guardValues, ...parentValues),
    ];
  }

  private parentReleaseStatement(
    path: string,
    row: StreamRow,
    childCount: number,
    childPath: string,
    childRow: StreamRow
  ): D1PreparedStatement {
    return this.db
      .prepare(
        `UPDATE streams SET child_count = ? WHERE ${guardedStreamWhere} AND EXISTS (SELECT 1 FROM streams WHERE ${guardedStreamWhere})`
      )
      .bind(
        childCount,
        ...streamMutationGuardValues(path, streamMutationGuard(row)),
        ...streamMutationGuardValues(childPath, streamMutationGuard(childRow))
      );
  }

  private async getStreamRow(path: string): Promise<StreamRow | null> {
    const row = await this.db
      .prepare("SELECT * FROM streams WHERE path = ?")
      .bind(path)
      .first<StreamRow>();

    if (!row) {
      return null;
    }

    if (isRowExpired(row)) {
      return await this.expireStream(path, row);
    }

    this.streamCache.set(path, { contentType: row.content_type });
    return row;
  }

  private async prepareCreate(options: PutOptions): Promise<PreparedCreate> {
    if (options.forkedFrom === undefined) {
      const prepared = prepareInitialData(options);
      this.assertAppendSize(prepared.data.length);
      return {
        ...prepared,
        incarnation: generateIncarnation(),
        contentType: resolveCreateContentType(options),
        ttlSeconds: options.ttlSeconds,
        expiresAt: options.expiresAt,
        closed: options.closed === true,
      };
    }

    return await this.prepareForkCreate(options, options.forkedFrom);
  }

  /**
   * copy a fork prefix into the child stream.
   * NOTE: linked parent chunks would save space, but v1 keeps delete and fork lifetime local by giving the child its own bytes.
   */
  private async prepareForkCreate(
    options: PutOptions,
    sourcePath: string
  ): Promise<PreparedCreate> {
    const source = await this.getStreamRow(sourcePath);
    if (!source) {
      throw new StreamNotFoundError(sourcePath);
    }
    if (source.deleted === 1) {
      throw new StreamConflictError("fork source is gone");
    }
    validateAppendContentType(source.content_type, options.contentType);

    const forkOffset = options.forkOffset ?? source.next_offset;
    const forkSubOffset = normalizeForkSubOffset(options.forkSubOffset);
    const sourceData = await this.readForkSourceBytes(
      sourcePath,
      source.next_offset,
      forkOffset,
      source.content_type,
      forkSubOffset,
      source.incarnation
    );
    let prepared: ReturnType<typeof prepareForkData>;
    try {
      prepared = prepareForkData(
        sourceData,
        forkOffset,
        source.next_offset,
        source.content_type,
        forkSubOffset,
        options.data
      );
    } catch (error) {
      this.rethrowTruncatedForkPayloadTooLarge(
        error,
        source.next_offset,
        forkSubOffset
      );
      throw error;
    }
    this.assertAppendSize(prepared.data.length);
    const { ttlSeconds, expiresAt } = inheritedExpiration(
      {
        ttlSeconds: source.ttl_seconds ?? undefined,
        expiresAt: source.expires_at ?? undefined,
      },
      options
    );

    return {
      ...prepared,
      incarnation: generateIncarnation(),
      contentType: source.content_type,
      ttlSeconds,
      expiresAt,
      closed: false,
      forkedFrom: sourcePath,
      forkOffset,
      forkSubOffset,
      forkSource: source,
    };
  }

  private idempotentCreateResult(
    existing: StreamRow,
    options: PutOptions
  ): PutResult {
    if (existing.deleted === 1) {
      throw new StreamConflictError("stream is gone");
    }

    validateIdempotentCreate(
      {
        contentType: existing.content_type,
        ttlSeconds: existing.ttl_seconds ?? undefined,
        expiresAt: existing.expires_at ?? undefined,
        closed: existing.closed === 1,
        forkedFrom: existing.forked_from ?? undefined,
        forkOffset: existing.fork_offset ?? undefined,
        forkSubOffset: existing.fork_sub_offset ?? undefined,
      },
      options
    );

    return {
      created: false,
      incarnation: existing.incarnation,
      nextOffset: existing.next_offset,
      contentType: existing.content_type,
      closed: existing.closed === 1,
    };
  }

  async put(path: string, options: PutOptions): Promise<PutResult> {
    const existing = await this.getStreamRow(path);

    if (existing) {
      assertStreamIncarnation(
        path,
        existing.incarnation,
        options.expectedIncarnation
      );
      return this.idempotentCreateResult(existing, options);
    }

    if (options.expectedIncarnation !== undefined) {
      throw new StreamConflictError(`stream incarnation is stale: ${path}`);
    }

    const prepared = await this.prepareCreate(options);
    const now = Date.now();
    const childGuard: StreamMutationGuard = {
      incarnation: prepared.incarnation,
      nextOffset: prepared.nextOffset,
      appendCount: prepared.appendCount,
      closed: prepared.closed ? 1 : 0,
      childCount: 0,
      deleted: 0,
      producerId: null,
      producerEpoch: 0,
      nextProducerSequence: 0,
    };
    const statements = [
      ...this.putStreamStatements(path, prepared, now),
      ...this.initialChunkStatements(
        path,
        prepared.data,
        prepared.appendCount,
        prepared.nextOffset,
        childGuard
      ),
    ];

    try {
      const results = await this.db.batch(statements);
      this.assertPutBatchResults(path, prepared, results);
    } catch (error) {
      if (isSqlPayloadTooLargeError(error)) {
        rethrowSqlPayloadTooLargeError(
          error,
          Math.min(prepared.data.length, this.maxChunkBytes)
        );
      }
      const existingAfterRace = await this.getStreamRow(path);
      if (existingAfterRace) {
        return this.idempotentCreateResult(existingAfterRace, options);
      }
      throw error;
    }

    this.streamCache.set(path, { contentType: prepared.contentType });
    return {
      created: true,
      incarnation: prepared.incarnation,
      nextOffset: prepared.nextOffset,
      contentType: prepared.contentType,
      closed: prepared.closed,
    };
  }

  private putStreamStatements(
    path: string,
    prepared: PreparedCreate,
    now: number
  ): D1PreparedStatement[] {
    if (
      prepared.forkSource === undefined ||
      prepared.forkedFrom === undefined
    ) {
      return [this.streamInsertStatement(path, prepared, now)];
    }

    const forkGuard = streamMutationGuard(prepared.forkSource);
    return [
      this.db
        .prepare(
          `UPDATE streams SET child_count = child_count + 1 WHERE ${guardedStreamWhere}`
        )
        .bind(...streamMutationGuardValues(prepared.forkedFrom, forkGuard)),
      this.guardedStreamInsertStatement(
        path,
        prepared,
        now,
        prepared.forkedFrom,
        {
          ...forkGuard,
          childCount: forkGuard.childCount + 1,
        }
      ),
    ];
  }

  private assertPutBatchResults(
    path: string,
    prepared: PreparedCreate,
    results: D1Result<unknown>[]
  ): void {
    if (
      prepared.forkSource === undefined ||
      prepared.forkedFrom === undefined
    ) {
      assertMutationChangedOneRow("create committed", path, results[0]);
      this.assertInitialChunkBatchResults(path, prepared, results, 1);
      return;
    }

    assertMutationChangedOneRow(
      "fork parent update committed",
      prepared.forkedFrom,
      results[0]
    );
    assertMutationChangedOneRow(
      "fork child create committed",
      path,
      results[1]
    );
    this.assertInitialChunkBatchResults(path, prepared, results, 2);
  }

  private assertInitialChunkBatchResults(
    path: string,
    prepared: PreparedCreate,
    results: D1Result<unknown>[],
    startIndex: number
  ): void {
    const expectedChunks =
      prepared.data.length === 0
        ? 0
        : Math.ceil(prepared.data.length / this.maxChunkBytes);
    for (let index = 0; index < expectedChunks; index += 1) {
      assertMutationChangedOneRow(
        "initial chunk committed",
        path,
        results[startIndex + index]
      );
    }
  }

  async append(
    path: string,
    data: Uint8Array,
    options?: AppendOptions
  ): Promise<AppendResult> {
    const stream = await this.getStreamRow(path);
    if (!stream) {
      throw new StreamNotFoundError(path);
    }
    assertStreamLive(path, { deleted: stream.deleted === 1 });
    assertStreamIncarnation(
      path,
      stream.incarnation,
      options?.expectedIncarnation
    );

    const producerDecision = await this.evaluateProducerDecision(
      stream,
      options?.producer
    );
    if (data.length > 0) {
      validateAppendContentType(stream.content_type, options?.contentType);
    }

    const append = this.prepareAppendChunk(
      data,
      stream.content_type,
      stream.append_count,
      stream.next_offset
    );
    const shouldClose = options?.close === true;
    this.assertMeaningfulAppend(append, shouldClose);

    if (producerDecision._tag === "Duplicate") {
      const retryEndOffset = await this.assertProducerRetryMatches(
        path,
        options?.producer,
        append,
        shouldClose,
        stream.incarnation
      );
      await this.touchStream(path, stream);
      return {
        incarnation: stream.incarnation,
        nextOffset: retryEndOffset,
        producer: producerDecision.result,
        closed: stream.closed === 1,
        appended: false,
      };
    }

    if (stream.closed === 1) {
      if (options?.producer !== undefined) {
        let retryEndOffset: Offset;
        try {
          retryEndOffset = await this.assertProducerRetryMatches(
            path,
            options.producer,
            append,
            shouldClose,
            stream.incarnation
          );
        } catch (error) {
          if (!(error instanceof StreamConflictError)) {
            throw error;
          }
          throw new StreamClosedError(path, stream.next_offset);
        }
        await this.touchStream(path, stream);
        return {
          incarnation: stream.incarnation,
          nextOffset: retryEndOffset,
          producer: { ...options.producer, duplicate: true },
          closed: true,
          appended: false,
        };
      }
      const closedResult = closedAppendResult(
        path,
        stream.incarnation,
        stream.next_offset,
        true,
        data,
        options,
        producerDecision
      );
      if (closedResult) {
        await this.touchStream(path, stream);
        return closedResult;
      }
    }
    validateAppendSeq(stream.last_seq ?? undefined, options?.seq);

    const statements = this.appendStatements(
      path,
      stream,
      append,
      producerDecision,
      options?.seq ?? stream.last_seq,
      shouldClose,
      this.nextLastAccessedAt(stream)
    );

    try {
      const results = await this.db.batch(statements);
      assertAppendCommitChangedOneRow(path, results.at(-1));
    } catch (error) {
      rethrowSqlPayloadTooLargeError(error, append.data.length);
    }

    await this.notifyWaitersAfterCommit(
      path,
      append.nextOffset,
      stream.content_type,
      stream.incarnation,
      shouldClose
    );

    return appendResult(
      stream.incarnation,
      append.nextOffset,
      shouldClose,
      append.appended,
      producerDecision
    );
  }

  async acquireProducer(
    path: string,
    producerId: string
  ): Promise<ProducerClaim> {
    if (producerId.length === 0) {
      throw new InvalidProducerError("Producer-Id must not be empty");
    }

    const stream = await this.getStreamRow(path);
    if (!stream) {
      throw new StreamNotFoundError(path);
    }
    assertStreamLive(path, { deleted: stream.deleted === 1 });

    const epoch = stream.producer_epoch + 1;
    const result = await this.db
      .prepare(
        `UPDATE streams SET producer_id = ?, producer_epoch = ?, next_producer_sequence = 0 WHERE ${guardedStreamWhere}`
      )
      .bind(
        producerId,
        epoch,
        ...streamMutationGuardValues(path, streamMutationGuard(stream))
      )
      .run();
    assertMutationChangedOneRow("producer claim committed", path, result);

    return {
      id: producerId,
      epoch,
      nextSeq: 0,
      incarnation: stream.incarnation,
      nextOffset: stream.next_offset,
    };
  }

  async get(path: string, options?: GetOptions): Promise<GetResult> {
    const stream = await this.getStreamRow(path);
    if (!stream) {
      throw new StreamNotFoundError(path);
    }
    assertStreamLive(path, { deleted: stream.deleted === 1 });
    assertStreamIncarnation(
      path,
      stream.incarnation,
      options?.expectedIncarnation
    );
    const readStream =
      options?.renewTtl === false
        ? stream
        : await this.touchStream(path, stream);

    const startOffset = options?.offset ?? initialOffset();
    const window = await this.readWindow(
      path,
      startOffset,
      readStream.next_offset,
      readStream.content_type,
      readStream.incarnation
    );

    return {
      messages: window.messages,
      incarnation: readStream.incarnation,
      nextOffset: window.nextOffset,
      upToDate: window.upToDate,
      cursor: calculateCursor(),
      etag: generateETag(path, startOffset, window.nextOffset),
      contentType: readStream.content_type,
      closed: readStream.closed === 1 && window.upToDate,
    };
  }

  async head(path: string): Promise<HeadResult | null> {
    const stream = await this.getStreamRow(path);
    if (!stream) {
      this.streamCache.delete(path);
      return null;
    }
    assertStreamLive(path, { deleted: stream.deleted === 1 });

    return {
      incarnation: stream.incarnation,
      contentType: stream.content_type,
      nextOffset: stream.next_offset,
      etag: generateETag(path, initialOffset(), stream.next_offset),
      closed: stream.closed === 1,
      ttlSeconds: stream.ttl_seconds ?? undefined,
      expiresAt: stream.expires_at ?? undefined,
    };
  }

  async delete(path: string): Promise<void> {
    const stream = await this.getStreamRow(path);
    if (!stream) {
      return;
    }

    assertStreamLive(path, { deleted: stream.deleted === 1 });

    if (stream.child_count > 0) {
      const result = await bindStreamMutationGuard(
        this.db.prepare(
          `UPDATE streams SET deleted = 1 WHERE ${guardedStreamWhere}`
        ),
        path,
        streamMutationGuard(stream)
      ).run();
      assertMutationChangedOneRow("delete committed", path, result);
      this.notifyDeleted(path);
      return;
    }

    await this.hardDelete(path, stream);
  }

  async has(path: string): Promise<boolean> {
    const stream = await this.getStreamRow(path);
    return stream !== null && stream.deleted !== 1;
  }

  async waitForData(
    path: string,
    offset: Offset,
    timeoutMs: number,
    options?: WaitOptions
  ): Promise<WaitResult> {
    const stream = await this.getStreamRow(path);
    if (!stream) {
      throw new StreamNotFoundError(path);
    }
    assertStreamLive(path, { deleted: stream.deleted === 1 });
    assertStreamIncarnation(
      path,
      stream.incarnation,
      options?.expectedIncarnation
    );
    const readStream =
      options?.renewTtl === false
        ? stream
        : await this.touchStream(path, stream);

    const window = await this.readWindow(
      path,
      offset,
      readStream.next_offset,
      readStream.content_type,
      readStream.incarnation
    );
    if (window.messages.length > 0) {
      return {
        messages: window.messages,
        timedOut: false,
        incarnation: readStream.incarnation,
        closed: readStream.closed === 1 && window.upToDate,
      };
    }

    if (readStream.closed === 1) {
      return {
        messages: [],
        timedOut: false,
        incarnation: readStream.incarnation,
        closed: true,
      };
    }

    const latestStream = await this.getStreamRow(path);
    if (
      latestStream === null ||
      latestStream.deleted === 1 ||
      latestStream.incarnation !== readStream.incarnation
    ) {
      return {
        messages: [],
        timedOut: false,
        incarnation: readStream.incarnation,
      };
    }
    const latestWindow = await this.readWindow(
      path,
      offset,
      latestStream.next_offset,
      latestStream.content_type,
      latestStream.incarnation
    );
    if (latestWindow.messages.length > 0) {
      return {
        messages: latestWindow.messages,
        timedOut: false,
        incarnation: latestStream.incarnation,
        closed: latestStream.closed === 1 && latestWindow.upToDate,
      };
    }
    if (latestStream.closed === 1) {
      return {
        messages: [],
        timedOut: false,
        incarnation: latestStream.incarnation,
        closed: true,
      };
    }

    const deferred = await Effect.runPromise(Deferred.make<WaitResult>());
    const waiter: Waiter = {
      deferred,
      offset,
      incarnation: latestStream.incarnation,
    };
    this.addWaiter(path, waiter);

    try {
      const parkedResult = await this.readParkedWaiterResult(
        path,
        offset,
        latestStream.incarnation
      );
      if (parkedResult !== undefined) {
        return parkedResult;
      }

      const timeout = Effect.as(Effect.delay(Effect.void, timeoutMs), {
        messages: [],
        timedOut: true,
        incarnation: latestStream.incarnation,
      } satisfies WaitResult);
      return await Effect.runPromise(
        Effect.race(Deferred.await(deferred), timeout)
      );
    } finally {
      this.removeWaiter(path, waiter);
    }
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

  private prepareAppendChunk(
    data: Uint8Array,
    contentType: string,
    appendCount: number,
    nextOffset: Offset
  ): PreparedAppendChunk {
    if (data.length === 0) {
      return { data, appendCount, nextOffset, appended: false };
    }

    const chunkData = isJsonContentType(contentType)
      ? processJsonAppend(new Uint8Array(0), data)
      : data;
    this.assertAppendSize(chunkData.length);

    const nextPos = offsetToBytePos(nextOffset) + chunkData.length;
    return {
      data: chunkData,
      appendCount: appendCount + 1,
      nextOffset: formatOffset(appendCount + 1, nextPos),
      appended: true,
    };
  }

  private assertChunkSize(size: number): void {
    if (size > this.maxChunkBytes) {
      throw new PayloadTooLargeError(this.maxChunkBytes, size);
    }
  }

  private assertAppendSize(size: number): void {
    if (size > this.maxAppendBytes) {
      throw new PayloadTooLargeError(this.maxAppendBytes, size);
    }
  }

  private assertMeaningfulAppend(
    append: PreparedAppendChunk,
    closed: boolean
  ): void {
    if (!append.appended && !closed) {
      throw new StreamConflictError("empty append must close the stream");
    }
  }

  private async getProducerState(
    stream: StreamRow,
    producer: AppendOptions["producer"]
  ): Promise<ProducerState | undefined> {
    if (producer === undefined) {
      return;
    }

    const row = await this.db
      .prepare(
        "SELECT epoch, seq FROM stream_producers WHERE path = ? AND incarnation = ? AND producer_id = ? AND EXISTS (SELECT 1 FROM streams WHERE streams.path = stream_producers.path AND streams.incarnation = stream_producers.incarnation AND streams.deleted = 0)"
      )
      .bind(stream.path, stream.incarnation, producer.id)
      .first<ProducerRow>();
    if (row) {
      return { epoch: row.epoch, seq: row.seq };
    }

    return;
  }

  private appendStatements(
    path: string,
    stream: StreamRow,
    append: PreparedAppendChunk,
    producerDecision: ProducerAppendDecision,
    lastSeq: string | null,
    closed: boolean,
    lastAccessedAt: number | null
  ): D1PreparedStatement[] {
    const statements: D1PreparedStatement[] = [];
    const guard: AppendCommitGuard = {
      ...appendCommitGuard(stream),
    };
    const nextProducerSequence =
      stream.producer_id !== null && producerDecision._tag === "Accepted"
        ? stream.next_producer_sequence + 1
        : stream.next_producer_sequence;

    if (append.appended) {
      statements.push(
        ...this.guardedAppendChunkStatements(
          path,
          stream.next_offset,
          append,
          guard
        )
      );
    }
    const producerStatement = this.producerStateStatement(
      path,
      producerDecision,
      guard,
      stream.producer_id !== null
    );
    if (producerStatement) {
      statements.push(producerStatement);
    }
    const producerAppendStatement = this.producerAppendStatement(
      path,
      stream.next_offset,
      append,
      producerDecision,
      guard,
      closed
    );
    if (producerAppendStatement) {
      statements.push(producerAppendStatement);
    }

    statements.push(
      this.db
        .prepare(
          `UPDATE streams SET next_offset = ?, append_count = ?, last_seq = ?, closed = ?, last_accessed_at = ?, next_producer_sequence = ? WHERE ${guardedAppendWhere}`
        )
        .bind(
          append.nextOffset,
          append.appendCount,
          lastSeq,
          closed ? 1 : 0,
          lastAccessedAt,
          nextProducerSequence,
          ...appendCommitGuardValues(path, guard)
        )
    );

    return statements;
  }

  private async evaluateProducerDecision(
    stream: StreamRow,
    producer: AppendOptions["producer"]
  ): Promise<ProducerAppendDecision> {
    if (stream.producer_id !== null) {
      return evaluateClaimedProducerAppend(
        {
          id: stream.producer_id,
          epoch: stream.producer_epoch,
          nextSeq: stream.next_producer_sequence,
          incarnation: stream.incarnation,
          nextOffset: stream.next_offset,
        },
        producer
      );
    }

    const producerState = await this.getProducerState(stream, producer);
    const producerStates =
      producer === undefined || producerState === undefined
        ? {}
        : { [producer.id]: producerState };
    return evaluateProducerAppend(producerStates, producer);
  }

  private producerStateStatement(
    path: string,
    decision: ProducerAppendDecision,
    guard: AppendCommitGuard,
    claimed: boolean
  ): D1PreparedStatement | undefined {
    if (claimed || decision._tag !== "Accepted") {
      return;
    }

    return this.db
      .prepare(
        `INSERT INTO stream_producers (path, incarnation, producer_id, epoch, seq)
         SELECT ?, ?, ?, ?, ?
         FROM streams
         WHERE ${guardedAppendWhere}
         ON CONFLICT(path, incarnation, producer_id) DO UPDATE SET
           epoch = excluded.epoch,
           seq = excluded.seq`
      )
      .bind(
        path,
        guard.incarnation,
        decision.result.id,
        decision.nextState.epoch,
        decision.nextState.seq,
        ...appendCommitGuardValues(path, guard)
      );
  }

  private producerAppendStatement(
    path: string,
    startOffset: Offset,
    append: PreparedAppendChunk,
    decision: ProducerAppendDecision,
    guard: AppendCommitGuard,
    closed: boolean
  ): D1PreparedStatement | undefined {
    if (decision._tag !== "Accepted") {
      return;
    }

    return this.db
      .prepare(
        `INSERT INTO stream_producer_appends (path, incarnation, producer_id, epoch, seq, append_index, start_offset, end_offset, data_length, closed)
         SELECT ?, ?, ?, ?, ?, ?, ?, ?, ?, ?
         FROM streams
         WHERE ${guardedAppendWhere}`
      )
      .bind(
        path,
        guard.incarnation,
        decision.result.id,
        decision.result.epoch,
        decision.result.seq,
        append.appendCount,
        startOffset,
        append.nextOffset,
        append.data.length,
        closed ? 1 : 0,
        ...appendCommitGuardValues(path, guard)
      );
  }

  private async assertProducerRetryMatches(
    path: string,
    producer: AppendOptions["producer"],
    append: PreparedAppendChunk,
    closed: boolean,
    incarnation: StreamIncarnation
  ): Promise<Offset> {
    if (producer === undefined) {
      throw new StreamConflictError(
        `producer retry receipt is missing: ${path}`
      );
    }

    const row = await this.db
      .prepare(
        `SELECT start_offset, end_offset, data_length, closed
         FROM stream_producer_appends
         WHERE path = ? AND incarnation = ? AND producer_id = ? AND epoch = ? AND seq = ?
         AND EXISTS (SELECT 1 FROM streams WHERE streams.path = stream_producer_appends.path AND streams.incarnation = ? AND streams.deleted = 0)`
      )
      .bind(
        path,
        incarnation,
        producer.id,
        producer.epoch,
        producer.seq,
        incarnation
      )
      .first<ProducerAppendRow>();
    if (row === null) {
      throw new StreamConflictError(
        `producer sequence has no stored append: ${path}`
      );
    }
    const receiptClosed = Number(row.closed) === 1;
    if (receiptClosed && closed) {
      return row.end_offset;
    }
    if (
      receiptClosed !== closed ||
      row.data_length !== append.data.length
    ) {
      throw new StreamConflictError(
        `producer sequence has conflicting content: ${path}`
      );
    }
    if (append.data.length > 0) {
      const stored = await this.readAppendBytes(
        path,
        row.start_offset,
        row.end_offset,
        incarnation
      );
      if (!bytesEqual(stored, append.data)) {
        throw new StreamConflictError(
          `producer sequence has conflicting content: ${path}`
        );
      }
    }
    return row.end_offset;
  }

  private streamInsertStatement(
    path: string,
    prepared: PreparedCreate,
    now: number
  ): D1PreparedStatement {
    return this.db
      .prepare(
        `INSERT INTO streams (path, incarnation, content_type, ttl_seconds, expires_at, created_at, last_accessed_at, next_offset, last_seq, producer_id, producer_epoch, next_producer_sequence, append_count, closed, forked_from, fork_offset, fork_sub_offset, child_count, deleted)
         VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`
      )
      .bind(
        path,
        prepared.incarnation,
        prepared.contentType,
        prepared.ttlSeconds ?? null,
        prepared.expiresAt ?? null,
        now,
        now,
        prepared.nextOffset,
        null,
        null,
        0,
        0,
        prepared.appendCount,
        prepared.closed ? 1 : 0,
        prepared.forkedFrom ?? null,
        prepared.forkOffset ?? null,
        prepared.forkSubOffset ?? null,
        0,
        0
      );
  }

  private guardedStreamInsertStatement(
    path: string,
    prepared: PreparedCreate,
    now: number,
    parentPath: string,
    parentGuard: StreamMutationGuard
  ): D1PreparedStatement {
    return this.db
      .prepare(
        `INSERT INTO streams (path, incarnation, content_type, ttl_seconds, expires_at, created_at, last_accessed_at, next_offset, last_seq, producer_id, producer_epoch, next_producer_sequence, append_count, closed, forked_from, fork_offset, fork_sub_offset, child_count, deleted)
         SELECT ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?
         FROM streams
         WHERE ${guardedStreamWhere}`
      )
      .bind(
        path,
        prepared.incarnation,
        prepared.contentType,
        prepared.ttlSeconds ?? null,
        prepared.expiresAt ?? null,
        now,
        now,
        prepared.nextOffset,
        null,
        null,
        0,
        0,
        prepared.appendCount,
        prepared.closed ? 1 : 0,
        prepared.forkedFrom ?? null,
        prepared.forkOffset ?? null,
        prepared.forkSubOffset ?? null,
        0,
        0,
        ...streamMutationGuardValues(parentPath, parentGuard)
      );
  }

  private initialChunkStatements(
    path: string,
    data: Uint8Array,
    appendIndex: number,
    finalOffset: Offset,
    guard: StreamMutationGuard
  ): D1PreparedStatement[] {
    if (data.length === 0) {
      return [];
    }

    const statements: D1PreparedStatement[] = [];
    const chunkCount = Math.ceil(data.length / this.maxChunkBytes);
    let chunkIndex = 0;
    let startPos = 0;
    while (startPos < data.length) {
      const endPos = Math.min(startPos + this.maxChunkBytes, data.length);
      const chunk = data.slice(startPos, endPos);
      const endOffset =
        endPos === data.length
          ? finalOffset
          : formatOffset(appendIndex, endPos);
      statements.push(
        this.guardedInitialChunkInsertStatement(
          path,
          appendIndex,
          chunkIndex,
          chunkCount,
          startPos,
          chunk,
          startPos === 0
            ? initialOffset()
            : formatOffset(appendIndex, startPos),
          endOffset,
          guard
        )
      );
      chunkIndex += 1;
      startPos = endPos;
    }

    return statements;
  }

  private guardedAppendChunkStatements(
    path: string,
    startOffset: Offset,
    append: PreparedAppendChunk,
    guard: AppendCommitGuard
  ): D1PreparedStatement[] {
    if (!append.appended) {
      return [];
    }

    const statements: D1PreparedStatement[] = [];
    const appendIndex = append.appendCount;
    const basePos = offsetToBytePos(startOffset);
    const chunkCount = Math.ceil(append.data.length / this.maxChunkBytes);
    let chunkIndex = 0;
    let offset = 0;
    while (offset < append.data.length) {
      const nextOffset = Math.min(
        offset + this.maxChunkBytes,
        append.data.length
      );
      const chunk = append.data.slice(offset, nextOffset);
      const startPos = basePos + offset;
      const endPos = basePos + nextOffset;
      statements.push(
        this.guardedChunkInsertStatement(
          path,
          appendIndex,
          chunkIndex,
          chunkCount,
          startPos,
          chunk,
          offset === 0 ? startOffset : formatOffset(appendIndex, startPos),
          nextOffset === append.data.length
            ? append.nextOffset
            : formatOffset(appendIndex, endPos),
          guard
        )
      );
      chunkIndex += 1;
      offset = nextOffset;
    }

    return statements;
  }

  private guardedInitialChunkInsertStatement(
    path: string,
    appendIndex: number,
    chunkIndex: number,
    chunkCount: number,
    startPos: number,
    data: Uint8Array,
    startOffset: Offset,
    endOffset: Offset,
    guard: StreamMutationGuard
  ): D1PreparedStatement {
    this.assertChunkSize(data.length);
    return this.db
      .prepare(
        `INSERT INTO stream_chunks (path, incarnation, append_index, chunk_index, chunk_count, start_pos, end_pos, start_offset, end_offset, data)
         SELECT ?, ?, ?, ?, ?, ?, ?, ?, ?, ?
         FROM streams
         WHERE ${guardedStreamWhere}`
      )
      .bind(
        path,
        guard.incarnation,
        appendIndex,
        chunkIndex,
        chunkCount,
        startPos,
        startPos + data.length,
        startOffset,
        endOffset,
        data,
        ...streamMutationGuardValues(path, guard)
      );
  }

  private guardedChunkInsertStatement(
    path: string,
    appendIndex: number,
    chunkIndex: number,
    chunkCount: number,
    startPos: number,
    data: Uint8Array,
    startOffset: Offset,
    endOffset: Offset,
    guard: AppendCommitGuard
  ): D1PreparedStatement {
    this.assertChunkSize(data.length);
    return this.db
      .prepare(
        `INSERT INTO stream_chunks (path, incarnation, append_index, chunk_index, chunk_count, start_pos, end_pos, start_offset, end_offset, data)
         SELECT ?, ?, ?, ?, ?, ?, ?, ?, ?, ?
         FROM streams
         WHERE ${guardedAppendWhere}`
      )
      .bind(
        path,
        guard.incarnation,
        appendIndex,
        chunkIndex,
        chunkCount,
        startPos,
        startPos + data.length,
        startOffset,
        endOffset,
        data,
        ...appendCommitGuardValues(path, guard)
      );
  }

  private async readBytes(
    path: string,
    tailOffset: Offset,
    incarnation: StreamIncarnation
  ): Promise<Uint8Array> {
    const messages = await this.readAllMessages(
      path,
      initialOffset(),
      tailOffset,
      incarnation
    );
    const total = messages.reduce(
      (acc, message) => acc + message.data.length,
      0
    );
    const result = new Uint8Array(total);
    let offset = 0;
    for (const message of messages) {
      result.set(message.data, offset);
      offset += message.data.length;
    }
    return result;
  }

  private async readAppendBytes(
    path: string,
    startOffset: Offset,
    endOffset: Offset,
    incarnation: StreamIncarnation
  ): Promise<Uint8Array> {
    const messages = (
      await this.readMessages(
        path,
        startOffset,
        offsetToBytePos(endOffset),
        true,
        incarnation
      )
    ).messages;
    const total = messages.reduce(
      (acc, message) => acc + message.data.length,
      0
    );
    const result = new Uint8Array(total);
    let offset = 0;
    for (const message of messages) {
      result.set(message.data, offset);
      offset += message.data.length;
    }
    return result;
  }

  private async readForkSourceBytes(
    path: string,
    sourceTailOffset: Offset,
    forkOffset: Offset,
    contentType: string,
    forkSubOffset: number | undefined,
    incarnation: StreamIncarnation
  ): Promise<Uint8Array> {
    const { byteOffset: forkPos, tailPos: sourceTailPos } = validateReadOffset(
      forkOffset,
      sourceTailOffset
    );
    if (sourceTailPos <= this.maxAppendBytes) {
      return await this.readBytes(path, sourceTailOffset, incarnation);
    }
    if (forkPos > this.maxAppendBytes) {
      throw new PayloadTooLargeError(this.maxAppendBytes, forkPos);
    }
    if (!isJsonContentType(contentType) && forkSubOffset !== undefined) {
      const requestedBytes = forkPos + forkSubOffset;
      if (requestedBytes > this.maxAppendBytes) {
        throw new PayloadTooLargeError(this.maxAppendBytes, requestedBytes);
      }
    }
    return await this.readPrefixBytes(
      path,
      Math.min(sourceTailPos, this.maxAppendBytes + 1),
      incarnation
    );
  }

  private rethrowTruncatedForkPayloadTooLarge(
    error: unknown,
    sourceTailOffset: Offset,
    forkSubOffset: number | undefined
  ): void {
    if (
      error instanceof InvalidOffsetError &&
      forkSubOffset !== undefined &&
      offsetToBytePos(sourceTailOffset) > this.maxAppendBytes
    ) {
      throw new PayloadTooLargeError(
        this.maxAppendBytes,
        this.maxAppendBytes + 1
      );
    }
  }

  private async readPrefixBytes(
    path: string,
    endPos: number,
    incarnation: StreamIncarnation
  ): Promise<Uint8Array> {
    const messages = (
      await this.readMessages(path, initialOffset(), endPos, false, incarnation)
    ).messages;
    const total = messages.reduce(
      (acc, message) => acc + message.data.length,
      0
    );
    const result = new Uint8Array(total);
    let offset = 0;
    for (const message of messages) {
      result.set(message.data, offset);
      offset += message.data.length;
    }
    return result;
  }

  private async readWindow(
    path: string,
    startOffset: Offset,
    tailOffset: Offset,
    contentType: string,
    incarnation: StreamIncarnation
  ): Promise<ReadWindow> {
    const { byteOffset: startPos, tailPos } = validateReadOffset(
      startOffset,
      tailOffset
    );
    if (startPos === tailPos) {
      return { messages: [], nextOffset: tailOffset, upToDate: true };
    }

    const isJson = isJsonContentType(contentType);
    let windowEndPos = Math.min(startPos + this.maxReadBytes, tailPos);
    if (isJson && windowEndPos < tailPos) {
      windowEndPos = await this.expandJsonWindowEndPos(
        path,
        windowEndPos,
        incarnation
      );
    }
    const result = await this.readMessages(
      path,
      startOffset,
      windowEndPos,
      isJson,
      incarnation
    );
    if (result.messages.length === 0) {
      throw new StreamConflictError("stream chunk range is incomplete");
    }

    return {
      messages: result.messages,
      nextOffset: windowEndPos === tailPos ? tailOffset : result.nextOffset,
      upToDate: windowEndPos === tailPos,
    };
  }

  /**
   * keeps JSON reads aligned to complete logical appends.
   * NOTE: byte windows may cut binary streams anywhere, but JSON responses never wrap a partial object in `[...]`.
   */
  private async expandJsonWindowEndPos(
    path: string,
    proposedEndPos: number,
    incarnation: StreamIncarnation
  ): Promise<number> {
    const row = await this.db
      .prepare(
        `SELECT append_index, chunk_index, chunk_count
         FROM stream_chunks
         WHERE path = ? AND incarnation = ?
           AND EXISTS (SELECT 1 FROM streams WHERE streams.path = stream_chunks.path AND streams.incarnation = stream_chunks.incarnation AND streams.deleted = 0)
           AND ((start_pos < ? AND end_pos > ?) OR (start_pos = ? AND chunk_index > 0))
         ORDER BY start_pos
         LIMIT 1`
      )
      .bind(path, incarnation, proposedEndPos, proposedEndPos, proposedEndPos)
      .first<{
        append_index: number;
        chunk_index: number;
        chunk_count: number;
      }>();
    if (row === null) {
      return proposedEndPos;
    }

    const end = await this.db
      .prepare(
        "SELECT MAX(end_pos) AS end_pos FROM stream_chunks WHERE path = ? AND incarnation = ? AND append_index = ? AND EXISTS (SELECT 1 FROM streams WHERE streams.path = stream_chunks.path AND streams.incarnation = stream_chunks.incarnation AND streams.deleted = 0)"
      )
      .bind(path, incarnation, row.append_index)
      .first<{ end_pos: number | null }>();
    const endPos = end?.end_pos;
    if (
      typeof endPos !== "number" ||
      !Number.isSafeInteger(endPos) ||
      endPos <= proposedEndPos
    ) {
      throw new StreamConflictError(
        `stream chunk range is incomplete: ${path}`
      );
    }
    return endPos;
  }

  private async readAllMessages(
    path: string,
    startOffset: Offset,
    tailOffset: Offset,
    incarnation: StreamIncarnation
  ): Promise<StreamMessage[]> {
    return (
      await this.readMessages(
        path,
        startOffset,
        offsetToBytePos(tailOffset),
        true,
        incarnation
      )
    ).messages;
  }

  private async readMessages(
    path: string,
    startOffset: Offset,
    endPos: number | undefined,
    requireCompleteGroups: boolean,
    incarnation: StreamIncarnation
  ): Promise<SqlChunkMessagesResult> {
    const startPos = offsetToBytePos(startOffset);
    return readSqlChunkMessages(
      path,
      startOffset,
      incarnation,
      await this.readChunkRows(path, startPos, incarnation, endPos),
      endPos,
      requireCompleteGroups
    );
  }

  private async readChunkRows(
    path: string,
    startPos: number,
    incarnation: StreamIncarnation,
    endPos?: number
  ): Promise<ChunkRow[]> {
    if (endPos === undefined) {
      const result = await this.db
        .prepare(
          `SELECT append_index, incarnation, chunk_index, chunk_count, start_pos, end_pos, start_offset, end_offset, data
           FROM stream_chunks
           WHERE path = ? AND incarnation = ? AND end_pos > ?
           AND EXISTS (SELECT 1 FROM streams WHERE streams.path = stream_chunks.path AND streams.incarnation = stream_chunks.incarnation AND streams.deleted = 0)
           ORDER BY start_pos`
        )
        .bind(path, incarnation, startPos)
        .all<ChunkRow>();

      return result.results;
    }

    const result = await this.db
      .prepare(
        `SELECT append_index, incarnation, chunk_index, chunk_count, start_pos, end_pos, start_offset, end_offset, data
         FROM stream_chunks
         WHERE path = ? AND incarnation = ? AND end_pos > ? AND start_pos < ?
         AND EXISTS (SELECT 1 FROM streams WHERE streams.path = stream_chunks.path AND streams.incarnation = stream_chunks.incarnation AND streams.deleted = 0)
         ORDER BY start_pos`
      )
      .bind(path, incarnation, startPos, endPos)
      .all<ChunkRow>();

    return result.results;
  }

  private addWaiter(path: string, waiter: Waiter): void {
    const pathWaiters = this.waiters.get(path) ?? [];
    pathWaiters.push(waiter);
    this.waiters.set(path, pathWaiters);
  }

  private removeWaiter(path: string, waiter: Waiter): void {
    const currentWaiters = this.waiters.get(path) ?? [];
    const index = currentWaiters.indexOf(waiter);
    if (index === -1) {
      return;
    }

    currentWaiters.splice(index, 1);
    if (currentWaiters.length === 0) {
      this.waiters.delete(path);
      return;
    }
    this.waiters.set(path, currentWaiters);
  }

  /**
   * reads once after a d1 waiter is registered.
   * NOTE: d1 awaits can interleave with appends, so parking before this check closes the notification gap.
   */
  private async readParkedWaiterResult(
    path: string,
    offset: Offset,
    incarnation: StreamIncarnation
  ): Promise<WaitResult | undefined> {
    const stream = await this.getStreamRow(path);
    if (
      stream === null ||
      stream.deleted === 1 ||
      stream.incarnation !== incarnation
    ) {
      return { messages: [], timedOut: false, incarnation };
    }

    const window = await this.readWindow(
      path,
      offset,
      stream.next_offset,
      stream.content_type,
      incarnation
    );
    if (window.messages.length > 0) {
      return {
        messages: window.messages,
        timedOut: false,
        incarnation,
        closed: stream.closed === 1 && window.upToDate,
      };
    }
    if (stream.closed === 1) {
      return { messages: [], timedOut: false, incarnation, closed: true };
    }
  }

  /**
   * wakes parked readers after the append transaction commits.
   * NOTE: a waiter read can detect storage corruption, but that must not turn an already committed append into a reported append failure.
   */
  private async notifyWaitersAfterCommit(
    path: string,
    tailOffset: Offset,
    contentType: string,
    incarnation: StreamIncarnation,
    closed = false
  ): Promise<void> {
    try {
      await this.notifyWaiters(
        path,
        tailOffset,
        contentType,
        incarnation,
        closed
      );
    } catch {
      const waiters = this.waiters.get(path) ?? [];
      this.waiters.delete(path);
      await Effect.runPromise(
        Effect.forEach(waiters, (waiter) =>
          Deferred.succeed(waiter.deferred, {
            messages: [],
            timedOut: false,
            incarnation,
          })
        )
      );
    }
  }

  private async notifyWaiters(
    path: string,
    tailOffset: Offset,
    contentType: string,
    incarnation: StreamIncarnation,
    closed = false
  ): Promise<void> {
    const waiters = this.waiters.get(path) ?? [];
    this.waiters.set(path, []);

    for (const waiter of waiters) {
      if (
        waiter.incarnation !== undefined &&
        waiter.incarnation !== incarnation
      ) {
        await Effect.runPromise(
          Deferred.succeed(waiter.deferred, {
            messages: [],
            timedOut: false,
            incarnation,
          })
        );
        continue;
      }

      let window: ReadWindow;
      try {
        window = await this.readWindow(
          path,
          waiter.offset,
          tailOffset,
          contentType,
          incarnation
        );
      } catch {
        await Effect.runPromise(
          Deferred.succeed(waiter.deferred, {
            messages: [],
            timedOut: false,
            incarnation,
          })
        );
        continue;
      }
      const closedInWindow = closed && window.upToDate;

      if (window.messages.length > 0 || closedInWindow) {
        await Effect.runPromise(
          Deferred.succeed(waiter.deferred, {
            messages: window.messages,
            timedOut: false,
            incarnation,
            closed: closedInWindow,
          })
        );
        continue;
      }

      const remaining = this.waiters.get(path) ?? [];
      remaining.push(waiter);
      this.waiters.set(path, remaining);
    }
  }

  private notifyDeleted(path: string): void {
    const waiters = this.waiters.get(path) ?? [];
    notifyDeletedWaiters(waiters);

    this.waiters.delete(path);
    this.streamCache.delete(path);
  }
}
