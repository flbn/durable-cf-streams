export type {
  Cursor,
  ETag,
  Offset,
  ProducerState,
  ProducerStateMap,
  StreamIncarnation,
} from "./schema.js";

import type { Cursor, ETag, Offset, StreamIncarnation } from "./schema.js";

export type StreamMessage = {
  readonly offset: Offset;
  readonly timestamp: number;
  readonly data: Uint8Array;
};

export type StreamMetadata = {
  readonly path: string;
  readonly incarnation: StreamIncarnation;
  readonly contentType: string;
  readonly ttlSeconds?: number;
  readonly expiresAt?: string;
  readonly createdAt: number;
  readonly lastAccessedAt?: number;
  readonly closed?: boolean;
  readonly forkedFrom?: string;
  readonly forkOffset?: Offset;
  readonly forkSubOffset?: number;
  readonly childCount?: number;
  readonly deleted?: boolean;
};

export type PutOptions = {
  readonly expectedIncarnation?: StreamIncarnation;
  readonly contentType?: string;
  readonly ttlSeconds?: number;
  readonly expiresAt?: string;
  readonly data?: Uint8Array;
  readonly closed?: boolean;
  readonly forkedFrom?: string;
  readonly forkOffset?: Offset;
  readonly forkSubOffset?: number;
};

export type PutResult = {
  readonly created: boolean;
  readonly incarnation: StreamIncarnation;
  readonly nextOffset: Offset;
  readonly contentType: string;
  readonly closed?: boolean;
};

export type AppendOptions = {
  readonly expectedIncarnation?: StreamIncarnation;
  readonly contentType?: string;
  readonly seq?: string;
  readonly producer?: ProducerAppendOptions;
  readonly close?: boolean;
};

export type AppendResult = {
  readonly incarnation: StreamIncarnation;
  readonly nextOffset: Offset;
  readonly producer?: ProducerAppendResult;
  readonly closed?: boolean;
  readonly appended?: boolean;
};

export type ProducerAppendOptions = {
  readonly id: string;
  readonly epoch: number;
  readonly seq: number;
};

export type ProducerAppendResult = {
  readonly id: string;
  readonly epoch: number;
  readonly seq: number;
  readonly duplicate: boolean;
};

export type ProducerClaim = {
  readonly id: string;
  readonly epoch: number;
  readonly nextSeq: number;
  readonly incarnation: StreamIncarnation;
  readonly nextOffset: Offset;
};

export type GetOptions = {
  readonly offset?: Offset;
  readonly expectedIncarnation?: StreamIncarnation;
  /** NOTE: set to false for live snapshots that do not perform metadata writes while they wait outside a serialized owner. */
  readonly renewTtl?: boolean;
};

export type GetResult = {
  readonly messages: StreamMessage[];
  readonly incarnation: StreamIncarnation;
  readonly nextOffset: Offset;
  readonly upToDate: boolean;
  readonly cursor: Cursor;
  readonly etag: ETag;
  readonly contentType: string;
  readonly closed: boolean;
};

export type HeadResult = {
  readonly incarnation: StreamIncarnation;
  readonly contentType: string;
  readonly nextOffset: Offset;
  readonly etag: ETag;
  readonly closed: boolean;
  readonly ttlSeconds?: number;
  readonly expiresAt?: string;
};

export type WaitResult = {
  readonly messages: StreamMessage[];
  readonly timedOut: boolean;
  readonly incarnation?: StreamIncarnation;
  readonly closed?: boolean;
};

export type WaitOptions = {
  readonly expectedIncarnation?: StreamIncarnation;
  /** NOTE: set to false for live waits that do not perform metadata writes while they wait outside a serialized owner. */
  readonly renewTtl?: boolean;
};
