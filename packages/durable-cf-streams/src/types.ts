export type Offset = string;

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
};

export type PutOptions = {
  readonly contentType: string;
  readonly ttlSeconds?: number;
  readonly expiresAt?: string;
  readonly data?: Uint8Array;
};

export type PutResult = {
  readonly created: boolean;
  readonly nextOffset: Offset;
};

export type AppendOptions = {
  readonly contentType?: string;
  readonly seq?: string;
};

export type AppendResult = {
  readonly nextOffset: Offset;
};

export type GetOptions = {
  readonly offset?: Offset;
};

export type GetResult = {
  readonly messages: StreamMessage[];
  readonly nextOffset: Offset;
  readonly upToDate: boolean;
  readonly cursor: string;
  readonly etag: string;
  readonly contentType: string;
};

export type HeadResult = {
  readonly contentType: string;
  readonly nextOffset: Offset;
  readonly etag: string;
};

export type WaitResult = {
  readonly messages: StreamMessage[];
  readonly timedOut: boolean;
};
