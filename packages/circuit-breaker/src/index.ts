import CircuitBreaker, { type Options as OpossumOptions, type State } from "opossum";
import { createLogger } from "@catalyst/logger";

const logger = createLogger({ name: "circuit-breaker" });

export interface BreakerOptions {
  name: string;
  timeout?: number;
  errorThresholdPercentage?: number;
  resetTimeout?: number;
  volumeThreshold?: number;
  rollingCountTimeout?: number;
  fallback?: (...args: unknown[]) => unknown;
}

export type AsyncFn<TArgs extends unknown[] = unknown[], TResult = unknown> = (...args: TArgs) => Promise<TResult>;

export class TrackedBreaker<TArgs extends unknown[], TResult> {
  private breaker: CircuitBreaker<TArgs, TResult>;
  readonly name: string;

  constructor(fn: AsyncFn<TArgs, TResult>, options: BreakerOptions) {
    this.name = options.name;
    const opossumOpts: OpossumOptions = {
      name: options.name,
      timeout: options.timeout ?? 10000,
      errorThresholdPercentage: options.errorThresholdPercentage ?? 50,
      resetTimeout: options.resetTimeout ?? 30000,
      rollingCountTimeout: options.rollingCountTimeout ?? 10000,
      volumeThreshold: options.volumeThreshold ?? 5,
    };
    this.breaker = new CircuitBreaker(fn, opossumOpts);

    if (options.fallback) {
      this.breaker.fallback(options.fallback);
    }

    this.breaker.on("open", () => logger.warn({ breaker: this.name }, "Circuit OPEN"));
    this.breaker.on("halfOpen", () => logger.info({ breaker: this.name }, "Circuit HALF_OPEN"));
    this.breaker.on("close", () => logger.info({ breaker: this.name }, "Circuit CLOSED"));
    this.breaker.on("reject", () => {});
    this.breaker.on("timeout", () => {});
  }

  fire(...args: TArgs): Promise<TResult> {
    return this.breaker.fire(...args);
  }

  get state(): State {
    return this.breaker.state;
  }

  get isOpen(): boolean {
    return this.breaker.opened;
  }

  get isClosed(): boolean {
    return this.breaker.closed;
  }

  get stats() {
    return this.breaker.stats;
  }

  shutdown(): void {
    this.breaker.shutdown();
  }
}

export function createBreaker<TArgs extends unknown[], TResult>(
  fn: AsyncFn<TArgs, TResult>,
  options: BreakerOptions,
): TrackedBreaker<TArgs, TResult> {
  return new TrackedBreaker(fn, options);
}
