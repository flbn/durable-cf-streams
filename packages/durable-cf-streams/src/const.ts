export const STREAM_OFFSET_HEADER = "Stream-Next-Offset";
export const STREAM_CURSOR_HEADER = "Stream-Cursor";
export const STREAM_UP_TO_DATE_HEADER = "Stream-Up-To-Date";
export const STREAM_SEQ_HEADER = "Stream-Seq";
export const STREAM_TTL_HEADER = "Stream-TTL";
export const STREAM_EXPIRES_AT_HEADER = "Stream-Expires-At";
export const STREAM_SSE_DATA_ENCODING_HEADER = "Stream-SSE-Data-Encoding";
export const STREAM_CLOSED_HEADER = "Stream-Closed";
export const PRODUCER_ID_HEADER = "Producer-Id";
export const PRODUCER_EPOCH_HEADER = "Producer-Epoch";
export const PRODUCER_SEQ_HEADER = "Producer-Seq";
export const PRODUCER_EXPECTED_SEQ_HEADER = "Producer-Expected-Seq";
export const PRODUCER_RECEIVED_SEQ_HEADER = "Producer-Received-Seq";
export const CACHE_CONTROL_HEADER = "Cache-Control";
export const CONTENT_TYPE_OPTIONS_HEADER = "X-Content-Type-Options";
export const CROSS_ORIGIN_RESOURCE_POLICY_HEADER =
  "Cross-Origin-Resource-Policy";

export const PROTOCOL_SECURITY_HEADERS = {
  [CONTENT_TYPE_OPTIONS_HEADER]: "nosniff",
  [CROSS_ORIGIN_RESOURCE_POLICY_HEADER]: "cross-origin",
} as const;

export const HEAD_CACHE_CONTROL_VALUE = "no-store";
export const SSE_CACHE_CONTROL_VALUE = "no-cache";

export const OFFSET_QUERY_PARAM = "offset";
export const TAIL_OFFSET_QUERY_VALUE = "now";
export const LIVE_QUERY_PARAM = "live";
export const CURSOR_QUERY_PARAM = "cursor";

export const SSE_COMPATIBLE_CONTENT_TYPES: readonly string[] = [
  "text/",
  "application/json",
];
