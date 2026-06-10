import { Schema } from "effect";

const OFFSET_REGEX = /^[0-9a-f]{16}_[0-9a-f]{16}$/;

const nonEmptyString = (name: string) =>
  Schema.String.pipe(
    Schema.filter(
      (value) => value.trim().length > 0 || `Expected non-empty ${name}`
    )
  );

const nonNegativeSafeInteger = (name: string) =>
  Schema.Number.pipe(
    Schema.filter(
      (value) =>
        (Number.isSafeInteger(value) && value >= 0) ||
        `Expected ${name} to be a non-negative safe integer`
    )
  );

const positiveSafeInteger = (name: string) =>
  Schema.Number.pipe(
    Schema.filter(
      (value) =>
        (Number.isSafeInteger(value) && value > 0) ||
        `Expected ${name} to be a positive safe integer`
    )
  );

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
  epoch: nonNegativeSafeInteger("producer epoch"),
  seq: nonNegativeSafeInteger("producer sequence"),
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
  contentType: nonEmptyString("content type"),
  ttlSeconds: Schema.optional(positiveSafeInteger("ttlSeconds")),
  expiresAt: Schema.optional(Schema.String),
  createdAt: nonNegativeSafeInteger("createdAt"),
  lastAccessedAt: Schema.optional(nonNegativeSafeInteger("lastAccessedAt")),
  nextOffset: OffsetSchema,
  lastSeq: Schema.optional(Schema.String),
  appendCount: nonNegativeSafeInteger("appendCount"),
  producers: ProducerStateMapSchema,
  closed: Schema.optional(Schema.Boolean),
  forkedFrom: Schema.optional(Schema.String),
  forkOffset: Schema.optional(OffsetSchema),
  forkSubOffset: Schema.optional(nonNegativeSafeInteger("forkSubOffset")),
  childCount: Schema.optional(nonNegativeSafeInteger("childCount")),
  deleted: Schema.optional(Schema.Boolean),
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
