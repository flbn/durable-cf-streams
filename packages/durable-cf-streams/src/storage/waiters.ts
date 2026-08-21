import { Deferred, Effect } from "effect";
import type { Offset, StreamIncarnation, WaitResult } from "../types.js";
import { readWholeValueWindow } from "./utils.js";

export type Waiter = {
  readonly deferred: Deferred.Deferred<WaitResult>;
  readonly offset: Offset;
  readonly incarnation: StreamIncarnation | undefined;
};

type WaiterList = {
  readonly add: (waiter: Waiter) => void;
  readonly remove: (waiter: Waiter) => void;
};

const waitResultFromData = (
  offset: Offset,
  data: Uint8Array,
  tailOffset: Offset,
  closed: boolean | undefined,
  maxReadBytes: number,
  contentType?: string,
  appendEndPositions: readonly number[] = []
): WaitResult | undefined => {
  const window = readWholeValueWindow(
    data,
    offset,
    tailOffset,
    closed,
    maxReadBytes,
    contentType,
    appendEndPositions
  );

  if (window.messages.length > 0) {
    return {
      messages: window.messages,
      timedOut: false,
      closed: window.closed,
    };
  }

  if (closed === true) {
    return { messages: [], timedOut: false, closed: true };
  }
};

export const waitForChange = (
  waiters: WaiterList,
  offset: Offset,
  timeoutMs: number,
  options?: { readonly incarnation?: StreamIncarnation }
): Promise<WaitResult> => {
  const effect = Effect.gen(function* () {
    const deferred = yield* Deferred.make<WaitResult>();
    const waiter: Waiter = {
      deferred,
      offset,
      incarnation: options?.incarnation,
    };
    waiters.add(waiter);

    const timeout = Effect.as(Effect.delay(Effect.void, timeoutMs), {
      messages: [],
      timedOut: true,
      incarnation: options?.incarnation,
    } satisfies WaitResult);

    const result = yield* Effect.race(Deferred.await(deferred), timeout);
    waiters.remove(waiter);

    return result;
  });

  return Effect.runPromise(effect);
};

export const notifyDataWaiters = (
  waiters: readonly Waiter[],
  data: Uint8Array,
  tailOffset: Offset,
  closed: boolean | undefined,
  maxReadBytes: number,
  contentType: string | undefined,
  appendEndPositions: readonly number[],
  requeue: (waiter: Waiter) => void
): void => {
  const effect = Effect.forEach(waiters, (waiter) => {
    const result = waitResultFromData(
      waiter.offset,
      data,
      tailOffset,
      closed,
      maxReadBytes,
      contentType,
      appendEndPositions
    );
    return result
      ? Deferred.succeed(waiter.deferred, result)
      : Effect.sync(() => requeue(waiter));
  });

  Effect.runSync(effect);
};

export const notifyDeletedWaiters = (waiters: readonly Waiter[]): void => {
  const effect = Effect.forEach(waiters, (waiter) =>
    Deferred.succeed(waiter.deferred, { messages: [], timedOut: false })
  );
  Effect.runSync(effect);
};
