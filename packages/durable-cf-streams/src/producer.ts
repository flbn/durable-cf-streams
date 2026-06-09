import {
  PRODUCER_EPOCH_HEADER,
  PRODUCER_ID_HEADER,
  PRODUCER_SEQ_HEADER,
} from "./const.js";
import {
  InvalidProducerError,
  ProducerFencedError,
  ProducerSequenceConflictError,
} from "./errors.js";
import type {
  ProducerAppendOptions,
  ProducerAppendResult,
  ProducerState,
  ProducerStateMap,
} from "./types.js";

const NON_NEGATIVE_INTEGER = /^(0|[1-9][0-9]*)$/;

export type ProducerAppendDecision =
  | { readonly _tag: "NoProducer" }
  | {
      readonly _tag: "Accepted";
      readonly nextState: ProducerState;
      readonly result: ProducerAppendResult;
    }
  | {
      readonly _tag: "Duplicate";
      readonly result: ProducerAppendResult;
    };

const parseProducerInteger = (name: string, value: string): number => {
  if (!NON_NEGATIVE_INTEGER.test(value)) {
    throw new InvalidProducerError(`${name} must be a non-negative integer`);
  }

  const parsed = Number.parseInt(value, 10);
  if (!Number.isSafeInteger(parsed)) {
    throw new InvalidProducerError(`${name} must be a safe integer`);
  }

  return parsed;
};

export const parseProducerHeaders = (
  headers: Headers
): ProducerAppendOptions | undefined => {
  const id = headers.get(PRODUCER_ID_HEADER);
  const epochValue = headers.get(PRODUCER_EPOCH_HEADER);
  const seqValue = headers.get(PRODUCER_SEQ_HEADER);

  if (id === null && epochValue === null && seqValue === null) {
    return;
  }

  if (id === null || epochValue === null || seqValue === null) {
    throw new InvalidProducerError(
      "Producer headers must be provided together"
    );
  }

  if (id.length === 0) {
    throw new InvalidProducerError("Producer-Id must not be empty");
  }

  return {
    id,
    epoch: parseProducerInteger(PRODUCER_EPOCH_HEADER, epochValue),
    seq: parseProducerInteger(PRODUCER_SEQ_HEADER, seqValue),
  };
};

export const evaluateProducerAppend = (
  producers: ProducerStateMap,
  producer: ProducerAppendOptions | undefined
): ProducerAppendDecision => {
  if (producer === undefined) {
    return { _tag: "NoProducer" };
  }

  const current = producers[producer.id];
  if (current === undefined) {
    if (producer.seq !== 0) {
      throw new ProducerSequenceConflictError("0", String(producer.seq));
    }

    return {
      _tag: "Accepted",
      nextState: { epoch: producer.epoch, seq: producer.seq },
      result: { ...producer, duplicate: false },
    };
  }

  if (producer.epoch < current.epoch) {
    throw new ProducerFencedError(current.epoch, producer.epoch);
  }

  if (producer.epoch > current.epoch) {
    if (producer.seq !== 0) {
      throw new InvalidProducerError(
        "Producer epoch upgrades must start at sequence 0"
      );
    }

    return {
      _tag: "Accepted",
      nextState: { epoch: producer.epoch, seq: producer.seq },
      result: { ...producer, duplicate: false },
    };
  }

  if (producer.seq <= current.seq) {
    return {
      _tag: "Duplicate",
      result: {
        id: producer.id,
        epoch: current.epoch,
        seq: current.seq,
        duplicate: true,
      },
    };
  }

  const expectedSeq = current.seq + 1;
  if (producer.seq !== expectedSeq) {
    throw new ProducerSequenceConflictError(
      String(expectedSeq),
      String(producer.seq)
    );
  }

  return {
    _tag: "Accepted",
    nextState: { epoch: producer.epoch, seq: producer.seq },
    result: { ...producer, duplicate: false },
  };
};

export const commitProducerAppend = (
  producers: ProducerStateMap,
  decision: ProducerAppendDecision
): ProducerStateMap =>
  decision._tag === "Accepted"
    ? { ...producers, [decision.result.id]: decision.nextState }
    : producers;
