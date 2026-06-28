import { BatchSpanProcessor } from "@opentelemetry/sdk-trace-base";
import { NodeTracerProvider } from "@opentelemetry/sdk-trace-node";
import { OTLPTraceExporter } from "@opentelemetry/exporter-trace-otlp-proto";
import { resourceFromAttributes } from "@opentelemetry/resources";
import { ATTR_SERVICE_NAME } from "@opentelemetry/semantic-conventions";
import { trace, context, propagation, type Tracer, type Span, type Attributes, type SpanOptions, type Context } from "@opentelemetry/api";
import { HttpInstrumentation } from "@opentelemetry/instrumentation-http";

let tracer: Tracer | null = null;
let provider: NodeTracerProvider | null = null;

export interface TracingOptions {
  serviceName: string;
  otlpEndpoint?: string;
}

export function initTracing(opts: TracingOptions): Tracer {
  if (tracer) return tracer;

  const exporter = new OTLPTraceExporter({
    url: opts.otlpEndpoint || process.env.OTEL_EXPORTER_OTLP_ENDPOINT || "http://localhost:4318/v1/traces",
  });

  const newProvider = new NodeTracerProvider({
    resource: resourceFromAttributes({
      [ATTR_SERVICE_NAME]: opts.serviceName,
    }),
    spanProcessors: [new BatchSpanProcessor(exporter)],
  });

  newProvider.register();
  provider = newProvider;

  try {
    new HttpInstrumentation({});
  } catch {}

  tracer = trace.getTracer(opts.serviceName);
  return tracer;
}

export async function shutdownTracing(): Promise<void> {
  if (provider) {
    const p = provider;
    provider = null;
    tracer = null;
    await p.shutdown();
  }
}

export function getTracer(): Tracer {
  if (!tracer) throw new Error("Tracing not initialized. Call initTracing() first.");
  return tracer;
}

export function startSpan(name: string, attrs?: Attributes, opts?: SpanOptions): Span {
  return getTracer().startSpan(name, { attributes: attrs, ...opts });
}

export function injectTraceHeaders(headers: Record<string, string> = {}): Record<string, string> {
  const carrier: Record<string, string> = {};
  propagation.inject(context.active(), carrier);
  const merged: Record<string, string> = { ...headers, ...carrier };
  return merged;
}

export function extractTraceFromHeaders(headers: Record<string, string | undefined>): Context | null {
  const carrier: Record<string, string> = {};
  for (const [k, v] of Object.entries(headers)) {
    if (v) carrier[k] = v;
  }
  return propagation.extract(context.active(), carrier);
}

export function startSpanWithTraceContext(
  headers: Record<string, string | undefined>,
  name: string,
  attrs?: Attributes,
  opts?: SpanOptions,
): Span {
  const ctx = extractTraceFromHeaders(headers);
  return ctx
    ? context.with(ctx, () => getTracer().startSpan(name, { attributes: attrs, ...opts }))
    : getTracer().startSpan(name, { attributes: attrs, ...opts });
}
