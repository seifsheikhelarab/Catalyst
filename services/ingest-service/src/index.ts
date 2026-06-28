import { Hono } from "hono";
import { getProducer, TOPICS, type Producer } from "@catalyst/kafka";
import { connectRedis } from "@catalyst/redis";
import { RawEventSchema, type RawEvent } from "@catalyst/types";
import { createLogger, flushLogs } from "@catalyst/logger";
import { initTracing, startSpan, injectTraceHeaders, shutdownTracing } from "@catalyst/tracing";
import { createCounter, createHistogram, metricsHandler } from "@catalyst/metrics";
import type { RedisClient } from "@catalyst/redis";
import type { ContentfulStatusCode } from "hono/utils/http-status";
import crypto from "crypto";

const logger = createLogger({ name: "ingest-service" });

const eventsReceived = createCounter({ name: "ingest_events_received_total", help: "Total events received" });
const eventsDeduped = createCounter({ name: "ingest_events_deduped_total", help: "Duplicate events rejected" });
const publishLatency = createHistogram({ name: "ingest_kafka_publish_latency_ms", help: "Kafka publish latency ms", buckets: [5, 10, 25, 50, 100, 250, 500] });

const INGRESS_TOPIC = TOPICS.RAW_EVENTS;
const DEDUP_TTL = 60;

let producer: Producer | null = null;

async function getKafkaProducer() {
  if (!producer) {
    producer = await getProducer();
  }
  return producer;
}

let redis: RedisClient | null = null;

async function getRedisClient() {
  if (!redis) {
    redis = await connectRedis();
  }
  return redis;
}

initTracing({ serviceName: "ingest-service" });

const app = new Hono();

async function computeDedupKey(event: RawEvent): Promise<string> {
  const hashInput = `${event.projectId}:${event.userId || ""}:${event.timestamp}:${event.event}`;
  return `dedup:${crypto.createHash("sha256").update(hashInput).digest("hex")}`;
}

async function processSingleEvent(eventData: Record<string, unknown>, clientIp: string): Promise<{ status: string; traceId?: string }> {
  // Inject client IP from gateway if not already set
  if (clientIp) {
    const props = (eventData.properties ?? {}) as Record<string, unknown>;
    eventData.properties = props;
    if (!props.ip && !props.clientIp) {
      props.clientIp = clientIp;
    }
  }

  const parseResult = RawEventSchema.safeParse(eventData);
  if (!parseResult.success) {
    throw { status: 400, error: "Invalid payload", details: parseResult.error.issues };
  }

  const event = parseResult.data as RawEvent;
  const span = startSpan("ingest.publish", { "event.type": event.event, "project.id": event.projectId });
  try {
    const redisClient = await getRedisClient();
    const dedupKey = await computeDedupKey(event);

    const existing = await redisClient.get(dedupKey);
    if (existing) {
      eventsDeduped.inc();
      logger.info({ dedupKey, event: event.event }, "Duplicate event rejected");
      return { status: "duplicate" };
    }

    eventsReceived.inc();

    const prod = await getKafkaProducer();
    const traceId = crypto.randomUUID();

    const message = {
      ...event,
      traceId,
    };

    const startTime = Date.now();
    const headers = injectTraceHeaders({ traceId });
    await prod.send({
      topic: INGRESS_TOPIC,
      messages: [
        {
          key: event.projectId,
          value: JSON.stringify(message),
          headers: Object.fromEntries(
            Object.entries(headers).map(([k, v]) => [k, Buffer.from(v)]),
          ),
        },
      ],
    });
    publishLatency.observe(Date.now() - startTime);

    await redisClient.set(dedupKey, "1", "EX", DEDUP_TTL);

    logger.info({ event: event.event, projectId: event.projectId, traceId }, "Event published");
    return { status: "accepted", traceId };
  } finally {
    span.end();
  }
}

app.post("/track", async (c) => {
  const body = await c.req.json();
  const clientIp = c.req.header("X-Forwarded-For") || "";

  // Accept both single event and batch array (from SDK)
  if (Array.isArray(body)) {
    const results: Array<{ status: string; traceId?: string }> = [];
    for (const item of body) {
      try {
        const result = await processSingleEvent(item, clientIp);
        results.push(result);
      } catch (err) {
        if (err && typeof err === "object" && "status" in err) {
          const apiErr = err as { status: number; details?: unknown };
          logger.warn({ error: apiErr.details }, "Batch event validation failed");
          results.push({ status: "rejected" });
        } else {
          logger.error({ error: err }, "Batch processing error");
          results.push({ status: "error" });
        }
      }
    }
    return c.json({ status: "batch", results }, 202);
  }

  try {
    const result = await processSingleEvent(body, clientIp);
    if (result.status === "duplicate") {
      return c.json(result, 202);
    }
    return c.json(result, 202);
  } catch (err) {
    if (err && typeof err === "object" && "status" in err) {
      const apiErr = err as { status: number; error: string; details?: unknown };
      return c.json({ error: apiErr.error, details: apiErr.details }, apiErr.status as ContentfulStatusCode);
    }
    throw err;
  }
});

app.get("/health", (c) => c.json({ status: "ok" }));
app.get("/metrics", (_c) => metricsHandler());

const port = parseInt(process.env.PORT || "3004");

async function shutdown() {
  logger.info("Shutting down...");
  await shutdownTracing();
  await flushLogs();
  process.exit(0);
}

process.on("SIGTERM", shutdown);
process.on("SIGINT", shutdown);

export default {
  port,
  fetch: app.fetch,
};
