type WaitResult = {
  readonly messages: Array<{
    readonly offset: string;
    readonly timestamp: number;
    readonly data: Uint8Array;
  }>;
  readonly timedOut: boolean;
  readonly streamClosed?: boolean;
};

type Waiter = {
  readonly offset: string;
  resolve: (result: WaitResult) => void;
  timer: ReturnType<typeof setTimeout>;
};

export class WaiterManager {
  private readonly waiters = new Map<string, Waiter[]>();

  wait(path: string, offset: string, timeoutMs: number): Promise<WaitResult> {
    return new Promise<WaitResult>((resolve) => {
      const timer = setTimeout(() => {
        this.removeWaiter(path, waiter);
        resolve({ messages: [], timedOut: true });
      }, timeoutMs);

      const waiter: Waiter = { offset, resolve, timer };
      const list = this.waiters.get(path) ?? [];
      list.push(waiter);
      this.waiters.set(path, list);
    });
  }

  notify(
    path: string,
    getData: (
      offset: string
    ) => { offset: string; timestamp: number; data: Uint8Array } | undefined
  ): void {
    const waiters = this.waiters.get(path);
    if (!waiters || waiters.length === 0) {
      return;
    }

    const remaining: Waiter[] = [];
    for (const waiter of waiters) {
      const msg = getData(waiter.offset);
      if (msg) {
        clearTimeout(waiter.timer);
        waiter.resolve({ messages: [msg], timedOut: false });
      } else {
        remaining.push(waiter);
      }
    }
    this.waiters.set(path, remaining);
  }

  notifyClosed(path: string): void {
    const waiters = this.waiters.get(path);
    if (!waiters) {
      return;
    }

    for (const waiter of waiters) {
      clearTimeout(waiter.timer);
      waiter.resolve({ messages: [], timedOut: false, streamClosed: true });
    }
    this.waiters.delete(path);
  }

  cancelForPath(path: string): void {
    const waiters = this.waiters.get(path);
    if (!waiters) {
      return;
    }

    for (const waiter of waiters) {
      clearTimeout(waiter.timer);
      waiter.resolve({ messages: [], timedOut: false });
    }
    this.waiters.delete(path);
  }

  cancelAll(): void {
    for (const [path] of this.waiters) {
      this.cancelForPath(path);
    }
    this.waiters.clear();
  }

  private removeWaiter(path: string, waiter: Waiter): void {
    const list = this.waiters.get(path);
    if (!list) {
      return;
    }
    const idx = list.indexOf(waiter);
    if (idx !== -1) {
      list.splice(idx, 1);
    }
  }
}
