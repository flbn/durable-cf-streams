import { Schema } from "effect";

const OFFSET_REGEX = /^[0-9a-f]{16}_[0-9a-f]{16}$/;

export const isOffsetString = (offset: string): boolean =>
  OFFSET_REGEX.test(offset);

export const OffsetSchema = Schema.String.pipe(
  Schema.filter(
    (offset) => isOffsetString(offset) || "Expected durable stream offset"
  ),
  Schema.brand("Offset")
);
export type Offset = Schema.Schema.Type<typeof OffsetSchema>;

export const CursorSchema = Schema.String.pipe(Schema.brand("Cursor"));
export type Cursor = Schema.Schema.Type<typeof CursorSchema>;

export const ETagSchema = Schema.String.pipe(Schema.brand("ETag"));
export type ETag = Schema.Schema.Type<typeof ETagSchema>;

export const ProducerStateSchema = Schema.Struct({
  epoch: Schema.Number,
  seq: Schema.Number,
});
export type ProducerState = Schema.Schema.Type<typeof ProducerStateSchema>;

export const ProducerStateMapSchema = Schema.Record({
  key: Schema.String,
  value: ProducerStateSchema,
});
export type ProducerStateMap = Schema.Schema.Type<
  typeof ProducerStateMapSchema
>;

export const PersistedStreamMetadataSchema = Schema.Struct({
  contentType: Schema.String,
  ttlSeconds: Schema.optional(Schema.Number),
  expiresAt: Schema.optional(Schema.String),
  createdAt: Schema.Number,
  nextOffset: OffsetSchema,
  lastSeq: Schema.optional(Schema.String),
  appendCount: Schema.Number,
  producers: ProducerStateMapSchema,
  closed: Schema.optional(Schema.Boolean),
});

export type PersistedStreamMetadata = Schema.Schema.Type<
  typeof PersistedStreamMetadataSchema
>;

export const decodePersistedStreamMetadata = Schema.decodeUnknownSync(
  PersistedStreamMetadataSchema
);

export const decodePersistedStreamMetadataJson = Schema.decodeUnknownSync(
  Schema.parseJson(PersistedStreamMetadataSchema)
);

export const decodeProducerStateMapJson = Schema.decodeUnknownSync(
  Schema.parseJson(ProducerStateMapSchema)
);
