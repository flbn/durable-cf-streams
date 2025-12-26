// biome-ignore lint: performance/noBarrelFile: bleh, its a library
export {
  compareCursors,
  generateCursor,
  getNextCursor,
  isNewerCursor,
  isValidCursor,
  parseCursor,
} from "./cursor.js";
export {
  ContentTypeMismatchError,
  InvalidJsonError,
  InvalidOffsetError,
  PayloadTooLargeError,
  SequenceConflictError,
  StreamConflictError,
  type StreamError,
  StreamNotFoundError,
} from "./errors.js";
export {
  advanceOffset,
  compareOffsets,
  formatOffset,
  incrementSeq,
  initialOffset,
  isSentinelOffset,
  isValidOffset,
  normalizeOffset,
  type ParsedOffset,
  parseOffset,
} from "./offsets.js";
export {
  formatJsonResponse,
  generateETag,
  isJsonContentType,
  normalizeContentType,
  parseETag,
  processJsonAppend,
  validateExpiresAt,
  validateJsonCreate,
  validateTTL,
} from "./protocol.js";
export type { StreamStore } from "./storage/interface.js";
export type {
  AppendOptions,
  AppendResult,
  GetOptions,
  GetResult,
  HeadResult,
  Offset,
  PutOptions,
  PutResult,
  StreamMessage,
  StreamMetadata,
  WaitResult,
} from "./types.js";
