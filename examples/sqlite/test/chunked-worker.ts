import { DurableObject } from "cloudflare:workers";
import {
  type AppendOptions,
  isStreamError,
  type Offset,
  type PutOptions,
  streamErrorStatus,
} from "durable-cf-streams";
import { ChunkedSqliteStore } from "durable-cf-streams/storage/chunked-sqlite";
import { SqliteStore } from "durable-cf-streams/storage/sqlite";

type Env = {
  STREAMS: DurableObjectNamespace<StreamDO>;
};

type Command =
  | {
      op: "put";
      path: string;
      contentType?: string;
      data?: string;
      closed?: boolean;
      forkedFrom?: string;
      forkOffset?: Offset;
      forkSubOffset?: number;
    }
  | {
      op: "append";
      path: string;
      data?: string;
      contentType?: string;
      seq?: string;
      close?: boolean;
      producer?: AppendOptions["producer"];
    }
  | { op: "get"; path: string; offset?: Offset }
  | { op: "head"; path: string }
  | { op: "delete"; path: string }
  | { op: "wait"; path: string; offset: Offset; timeoutMs?: number }
  | { op: "stats"; path: string }
  | { op: "seedLegacy"; path: string; data: string; contentType?: string }
  | { op: "snapshotTooLarge"; path: string; size: number };

const encoder = new TextEncoder();
const decoder = new TextDecoder();
const MAX_CHUNK_BYTES = 512_000;

const encodeData = (data: string | undefined): Uint8Array | undefined =>
  data === undefined ? undefined : encoder.encode(data);

const json = (body: unknown, init?: ResponseInit): Response =>
  Response.json(body, init);

export default {
  fetch(request: Request, env: Env): Response {
    const id = env.STREAMS.idFromName("chunked-store-test");
    return env.STREAMS.get(id).fetch(request);
  },
};

export class StreamDO extends DurableObject<Env> {
  private readonly store: ChunkedSqliteStore;

  constructor(state: DurableObjectState, env: Env) {
    super(state, env);
    this.store = new ChunkedSqliteStore(state.storage.sql, {
      maxChunkBytes: MAX_CHUNK_BYTES,
    });
    this.store.initialize();
  }

  async fetch(request: Request): Promise<Response> {
    try {
      const command = (await request.json()) as Command;
      return await this.handle(command);
    } catch (error) {
      if (isStreamError(error)) {
        return json(
          {
            error: {
              tag: error._tag,
              message: error.message,
              maxBytes: "maxBytes" in error ? error.maxBytes : undefined,
              receivedBytes:
                "receivedBytes" in error ? error.receivedBytes : undefined,
            },
          },
          { status: streamErrorStatus(error) }
        );
      }

      throw error;
    }
  }

  private async handle(command: Command): Promise<Response> {
    switch (command.op) {
      case "put": {
        const options: PutOptions = {
          contentType: command.contentType,
          data: encodeData(command.data),
          closed: command.closed,
          forkedFrom: command.forkedFrom,
          forkOffset: command.forkOffset,
          forkSubOffset: command.forkSubOffset,
        };
        const result = await this.store.put(command.path, options);
        return json(result);
      }
      case "append": {
        const data = encodeData(command.data) ?? new Uint8Array(0);
        const result = await this.store.append(command.path, data, {
          contentType: command.contentType,
          seq: command.seq,
          close: command.close,
          producer: command.producer,
        });
        return json(result);
      }
      case "get": {
        const result = await this.store.get(command.path, {
          offset: command.offset,
        });
        const body = this.store.formatResponse(command.path, result.messages);
        return json({
          body: decoder.decode(body),
          nextOffset: result.nextOffset,
          upToDate: result.upToDate,
          closed: result.closed,
        });
      }
      case "head": {
        return json(await this.store.head(command.path));
      }
      case "delete": {
        await this.store.delete(command.path);
        return json({ deleted: true });
      }
      case "wait": {
        const result = await this.store.waitForData(
          command.path,
          command.offset,
          command.timeoutMs ?? 5000
        );
        const body = this.store.formatResponse(command.path, result.messages);
        return json({
          body: decoder.decode(body),
          timedOut: result.timedOut,
          closed: result.closed,
        });
      }
      case "stats": {
        const streamRows = this.ctx.storage.sql
          .exec<{ legacy_size: number }>(
            "SELECT length(data) AS legacy_size FROM streams WHERE path = ?",
            command.path
          )
          .toArray();
        const chunks = this.ctx.storage.sql
          .exec<{ size: number }>(
            `SELECT length(data) AS size
             FROM stream_chunks
             WHERE path = ?
             ORDER BY start_pos`,
            command.path
          )
          .toArray();

        return json({
          maxChunkBytes: MAX_CHUNK_BYTES,
          legacyBytes: streamRows[0]?.legacy_size ?? 0,
          chunkBytes: chunks.reduce((total, row) => total + row.size, 0),
          maxStoredChunkBytes: chunks.reduce(
            (max, row) => Math.max(max, row.size),
            0
          ),
        });
      }
      case "seedLegacy": {
        const data = encoder.encode(command.data);
        const contentType = command.contentType ?? "text/plain";
        const now = Date.now();
        const nextOffset = offset(1, data.length);

        this.ctx.storage.sql.exec(
          "DELETE FROM stream_chunks WHERE path = ?",
          command.path
        );
        this.ctx.storage.sql.exec(
          "DELETE FROM streams WHERE path = ?",
          command.path
        );
        this.ctx.storage.sql.exec(
          `INSERT INTO streams (path, content_type, ttl_seconds, expires_at, created_at, last_accessed_at, data, next_offset, producers, append_count, closed, forked_from, fork_offset, fork_sub_offset, child_count, deleted)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
          command.path,
          contentType,
          null,
          null,
          now,
          now,
          data,
          nextOffset,
          "{}",
          1,
          0,
          null,
          null,
          null,
          0,
          0
        );

        return json({ nextOffset });
      }
      case "snapshotTooLarge": {
        const store = new SqliteStore(this.ctx.storage.sql);
        store.initialize();
        await store.put(command.path, { contentType: "text/plain" });
        await store.append(
          command.path,
          encoder.encode("x".repeat(command.size)),
          { contentType: "text/plain" }
        );
        return json({ appended: true });
      }
      default: {
        const _exhaustive: never = command;
        return json({ error: `Unknown op: ${_exhaustive}` }, { status: 400 });
      }
    }
  }
}

const offset = (seq: number, pos: number): Offset =>
  `${seq.toString(16).padStart(16, "0")}_${pos
    .toString(16)
    .padStart(16, "0")}` as Offset;
