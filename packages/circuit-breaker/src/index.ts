import CircuitBreaker, { type Options as OpossumOptions } from "opossum";
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

export function createBreaker<TArgs extends unknown[], TResult>(
  fn: (...args: TArgs) => Promise<TResult>,
  options: BreakerOptions,
) {
  const opossumOpts: OpossumOptions = {
    name: options.name,
    timeout: options.timeout ?? 10000,
    errorThresholdPercentage: options.errorThresholdPercentage ?? 50,
    resetTimeout: options.resetTimeout ?? 30000,
    rollingCountTimeout: options.rollingCountTimeout ?? 10000,
    volumeThreshold: options.volumeThreshold ?? 5,
  };
  const breaker = new CircuitBreaker(fn, opossumOpts);

  if (options.fallback) {
    breaker.fallback(options.fallback as (...args: TArgs) => TResult | Promise<TResult>);
  }

  breaker.on("open", () => logger.warn({ breaker: options.name }, "Circuit OPEN"));
  breaker.on("halfOpen", () => logger.info({ breaker: options.name }, "Circuit HALF_OPEN"));
  breaker.on("close", () => logger.info({ breaker: options.name }, "Circuit CLOSED"));

  return breaker;
}
