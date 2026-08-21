import type {
  AppendOptions,
  AppendResult,
  GetOptions,
  GetResult,
  HeadResult,
  Offset,
  ProducerClaim,
  PutOptions,
  PutResult,
  StreamMessage,
  WaitOptions,
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

  has(path: string): Promise<boolean>;

  waitForData(
    path: string,
    offset: Offset,
    timeoutMs: number,
    options?: WaitOptions
  ): Promise<WaitResult>;

  formatResponse(path: string, messages: StreamMessage[]): Uint8Array;

  acquireProducer?(path: string, producerId: string): Promise<ProducerClaim>;
};
