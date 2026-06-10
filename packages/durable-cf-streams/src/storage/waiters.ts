import { Deferred, Effect } from "effect";
import { offsetToBytePos } from "../offsets.js";
import type { Offset, WaitResult } from "../types.js";

export type Waiter = {
  readonly deferred: Deferred.Deferred<WaitResult>;
  readonly offset: Offset;
};

type WaiterList = {
  readonly add: (waiter: Waiter) => void;
  readonly remove: (waiter: Waiter) => void;
};

const timeoutResult: WaitResult = { messages: [], timedOut: true };

const waitResultFromData = (
  offset: Offset,
  data: Uint8Array,
  closed: boolean | undefined
): WaitResult | undefined => {
  const byteOffset = offsetToBytePos(offset);

  if (byteOffset < data.length) {
    return {
      messages: [
        { offset, timestamp: Date.now(), data: data.slice(byteOffset) },
      ],
      timedOut: false,
      closed,
    };
  }

  if (closed === true) {
    return { messages: [], timedOut: false, closed: true };
  }
};

export const waitForChange = (
  waiters: WaiterList,
  offset: Offset,
  timeoutMs: number
): Promise<WaitResult> => {
  const effect = Effect.gen(function* () {
    const deferred = yield* Deferred.make<WaitResult>();
    const waiter: Waiter = { deferred, offset };
    waiters.add(waiter);

    const timeout = Effect.as(
      Effect.delay(Effect.void, timeoutMs),
      timeoutResult
    );

    const result = yield* Effect.race(Deferred.await(deferred), timeout);
    waiters.remove(waiter);

    return result;
  });

  return Effect.runPromise(effect);
};

export const notifyDataWaiters = (
  waiters: readonly Waiter[],
  data: Uint8Array,
  closed: boolean | undefined,
  requeue: (waiter: Waiter) => void
): void => {
  const effect = Effect.forEach(waiters, (waiter) => {
    const result = waitResultFromData(waiter.offset, data, closed);
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
