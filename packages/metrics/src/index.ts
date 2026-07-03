import {
  Counter,
  Gauge,
  Histogram,
  Registry,
  type CounterConfiguration,
  type GaugeConfiguration,
  type HistogramConfiguration,
} from "prom-client";

const registry = new Registry();

export function createCounter(config: CounterConfiguration<string>): Counter<string> {
  return new Counter({ ...config, registers: [registry] });
}

export function createGauge(config: GaugeConfiguration<string>): Gauge<string> {
  return new Gauge({ ...config, registers: [registry] });
}

export function createHistogram(config: HistogramConfiguration<string>): Histogram<string> {
  return new Histogram({ ...config, registers: [registry] });
}

export async function metricsHandler(): Promise<Response> {
  return new Response(await registry.metrics(), {
    headers: { "Content-Type": registry.contentType },
  });
}

export function metricsContentType(): string {
  return registry.contentType;
}
