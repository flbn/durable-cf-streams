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

export const PersistedStreamMetadataSchema = Schema.Struct({
  contentType: Schema.String,
  ttlSeconds: Schema.optional(Schema.Number),
  expiresAt: Schema.optional(Schema.String),
  createdAt: Schema.Number,
  nextOffset: OffsetSchema,
  lastSeq: Schema.optional(Schema.String),
  appendCount: Schema.Number,
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
