export const DEFAULT_CURSOR_EPOCH: Date = new Date("2024-10-09T00:00:00.000Z");
export const DEFAULT_CURSOR_INTERVAL_SECONDS = 20;

const MAX_JITTER_SECONDS = 3600;
const MIN_JITTER_SECONDS = 1;

export type CursorOptions = {
  readonly intervalSeconds?: number;
  readonly epoch?: Date;
};

export const calculateCursor = (options: CursorOptions = {}): string => {
  const intervalSeconds =
    options.intervalSeconds ?? DEFAULT_CURSOR_INTERVAL_SECONDS;
  const epoch = options.epoch ?? DEFAULT_CURSOR_EPOCH;

  const now = Date.now();
  const epochMs = epoch.getTime();
  const intervalMs = intervalSeconds * 1000;

  const intervalNumber = Math.floor((now - epochMs) / intervalMs);
  return String(intervalNumber);
};

const generateJitterIntervals = (intervalSeconds: number): number => {
  const jitterSeconds =
    MIN_JITTER_SECONDS +
    Math.floor(Math.random() * (MAX_JITTER_SECONDS - MIN_JITTER_SECONDS + 1));
  return Math.max(1, Math.ceil(jitterSeconds / intervalSeconds));
};

export const generateResponseCursor = (
  clientCursor: string | undefined,
  options: CursorOptions = {}
): string => {
  const intervalSeconds =
    options.intervalSeconds ?? DEFAULT_CURSOR_INTERVAL_SECONDS;
  const currentCursor = calculateCursor(options);
  const currentInterval = Number.parseInt(currentCursor, 10);

  if (!clientCursor) {
    return currentCursor;
  }

  const clientInterval = Number.parseInt(clientCursor, 10);

  if (Number.isNaN(clientInterval) || clientInterval < currentInterval) {
    return currentCursor;
  }

  const jitterIntervals = generateJitterIntervals(intervalSeconds);
  return String(clientInterval + jitterIntervals);
};
