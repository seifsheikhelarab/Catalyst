declare module "opossum" {
  export type State = "open" | "closed" | "halfOpen" | "disabled" | "forcedOpen";

  export interface Options {
    name?: string;
    timeout?: number | false;
    errorThresholdPercentage?: number;
    resetTimeout?: number;
    rollingCountTimeout?: number;
    rollingCountBuckets?: number;
    volumeThreshold?: number;
    errorFilter?: (err: any) => boolean;
    cache?: boolean;
    cacheExpiry?: number;
    capacity?: number;
  }

  export interface Stats {
    fires: number;
    successes: number;
    failures: number;
    rejects: number;
    timeouts: number;
    cacheHits: number;
    cacheMisses: number;
    semaphoreRejections: number;
    percentiles: Record<string, number>;
    latencyTimes: number[];
    latencyMean: number;
    latencyWindow: number[];
    state: State;
  }

  export class CircuitBreaker<TArgs extends any[] = any[], TResult = any> {
    constructor(fn: (...args: TArgs) => Promise<TResult> | TResult, options?: Options);
    fire(...args: TArgs): Promise<TResult>;
    fallback(fallbackFn: (...args: TArgs) => Promise<TResult> | TResult): this;
    on(event: "open" | "close" | "halfOpen" | "reject" | "timeout" | "success" | "failure" | "fire" | "fallback" | "snapshot", handler: (...args: any[]) => void): this;
    enabled: boolean;
    closed: boolean;
    opened: boolean;
    isOpen: boolean;
    halfOpen: boolean;
    warmUp: boolean;
    readonly name: string;
    readonly stats: Stats;
    readonly state: State;
    shutdown(): void;
  }

  export type AsyncFn<TArgs extends any[] = any[], TResult = any> = (...args: TArgs) => Promise<TResult>;

  export default CircuitBreaker;
}
