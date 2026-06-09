export type {
  Cursor,
  ETag,
  Offset,
  ProducerState,
  ProducerStateMap,
} from "./schema.js";

import type { Cursor, ETag, Offset } from "./schema.js";

export type StreamMessage = {
  readonly offset: Offset;
  readonly timestamp: number;
  readonly data: Uint8Array;
};

export type StreamMetadata = {
  readonly path: string;
  readonly contentType: string;
  readonly ttlSeconds?: number;
  readonly expiresAt?: string;
  readonly createdAt: number;
  readonly lastAccessedAt?: number;
  readonly closed?: boolean;
  readonly forkedFrom?: string;
  readonly forkOffset?: Offset;
  readonly childCount?: number;
  readonly deleted?: boolean;
};

export type PutOptions = {
  readonly contentType: string;
  readonly ttlSeconds?: number;
  readonly expiresAt?: string;
  readonly data?: Uint8Array;
  readonly closed?: boolean;
  readonly forkedFrom?: string;
  readonly forkOffset?: Offset;
};

export type PutResult = {
  readonly created: boolean;
  readonly nextOffset: Offset;
  readonly closed?: boolean;
};

export type AppendOptions = {
  readonly contentType?: string;
  readonly seq?: string;
  readonly producer?: ProducerAppendOptions;
  readonly close?: boolean;
};

export type AppendResult = {
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

export type GetOptions = {
  readonly offset?: Offset;
};

export type GetResult = {
  readonly messages: StreamMessage[];
  readonly nextOffset: Offset;
  readonly upToDate: boolean;
  readonly cursor: Cursor;
  readonly etag: ETag;
  readonly contentType: string;
  readonly closed: boolean;
};

export type HeadResult = {
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
  readonly closed?: boolean;
};
