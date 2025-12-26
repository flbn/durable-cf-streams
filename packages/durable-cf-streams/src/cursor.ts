const CURSOR_INTERVAL_MS = 20_000;

export const generateCursor = (): string => {
  const interval = Math.floor(Date.now() / CURSOR_INTERVAL_MS);
  return interval.toString(10);
};

export const parseCursor = (cursor: string): number | null => {
  const parsed = Number.parseInt(cursor, 10);
  return Number.isNaN(parsed) || parsed <= 0 ? null : parsed;
};

export const isValidCursor = (cursor: string): boolean =>
  parseCursor(cursor) !== null;

export const compareCursors = (a: string, b: string): number => {
  const parsedA = Number.parseInt(a, 10);
  const parsedB = Number.parseInt(b, 10);
  return parsedA - parsedB;
};

export const isNewerCursor = (
  cursor: string,
  clientCursor: string | undefined
): boolean => {
  if (!clientCursor) {
    return true;
  }

  return compareCursors(cursor, clientCursor) > 0;
};

export const getNextCursor = (clientCursor: string | undefined): string => {
  const current = generateCursor();
  if (!clientCursor) {
    return current;
  }

  const clientInterval = Number.parseInt(clientCursor, 10);
  const currentInterval = Number.parseInt(current, 10);

  if (currentInterval <= clientInterval) {
    return (clientInterval + 1).toString(10);
  }
  return current;
};
