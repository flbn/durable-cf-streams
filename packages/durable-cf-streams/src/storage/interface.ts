import type {
  AppendOptions,
  AppendResult,
  GetOptions,
  GetResult,
  HeadResult,
  Offset,
  PutOptions,
  PutResult,
  StreamMessage,
  WaitResult,
} from "../types.js";

export type StreamStore = {
  put(path: string, options: PutOptions): Promise<PutResult>;

  append(
    path: string,
    data: Uint8Array,
    options?: AppendOptions
  ): Promise<AppendResult>;

  get(path: string, options?: GetOptions): Promise<GetResult>;

  head(path: string): Promise<HeadResult | null>;

  delete(path: string): Promise<void>;

  has(path: string): boolean;

  waitForData(
    path: string,
    offset: Offset,
    timeoutMs: number
  ): Promise<WaitResult>;

  formatResponse(path: string, messages: StreamMessage[]): Uint8Array;
};
