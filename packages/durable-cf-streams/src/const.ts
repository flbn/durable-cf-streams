export const STREAM_OFFSET_HEADER = "Stream-Next-Offset";
export const STREAM_CURSOR_HEADER = "Stream-Cursor";
export const STREAM_UP_TO_DATE_HEADER = "Stream-Up-To-Date";
export const STREAM_SEQ_HEADER = "Stream-Seq";
export const STREAM_TTL_HEADER = "Stream-TTL";
export const STREAM_EXPIRES_AT_HEADER = "Stream-Expires-At";

export const OFFSET_QUERY_PARAM = "offset";
export const LIVE_QUERY_PARAM = "live";
export const CURSOR_QUERY_PARAM = "cursor";

export const SSE_COMPATIBLE_CONTENT_TYPES: readonly string[] = [
  "text/",
  "application/json",
];
