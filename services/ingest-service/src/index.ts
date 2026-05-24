import { Hono } from "hono";
import { getProducer, TOPICS } from "@catalyst/kafka";
import { connectRedis } from "@catalyst/redis";
import { RawEventSchema, type RawEvent } from "@catalyst/types";
import { createLogger } from "@catalyst/logger";
import { initTracing, startSpan } from "@catalyst/tracing";
import { createCounter, createHistogram, metricsHandler } from "@catalyst/metrics";
import crypto from "crypto";

const logger = createLogger({ name: "ingest-service" });

const eventsReceived = createCounter({ name: "ingest_events_received_total", help: "Total events received" });
const eventsDeduped = createCounter({ name: "ingest_events_deduped_total", help: "Duplicate events rejected" });
const publishLatency = createHistogram({ name: "ingest_kafka_publish_latency_ms", help: "Kafka publish latency ms", buckets: [5, 10, 25, 50, 100, 250, 500] });

const INGRESS_TOPIC = TOPICS.RAW_EVENTS;
const DEDUP_TTL = 60;

let producer: any = null;

async function getKafkaProducer() {
  if (!producer) {
    producer = await getProducer();
  }
  return producer;
}

let redis: any = null;

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

app.post("/track", async (c) => {
  const body = await c.req.json();

  const parseResult = RawEventSchema.safeParse(body);

  if (!parseResult.success) {
    return c.json({ error: "Invalid payload", details: parseResult.error.issues }, 400);
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
      return c.json({ status: "duplicate" }, 202);
    }

    eventsReceived.inc();

    const prod = await getKafkaProducer();
    const traceId = crypto.randomUUID();

    const message = {
      ...event,
      traceId,
    };

    const startTime = Date.now();
    await prod.send({
      topic: INGRESS_TOPIC,
      messages: [
        {
          key: event.projectId,
          value: JSON.stringify(message),
          headers: {
            traceId,
          },
        },
      ],
    });
    publishLatency.observe(Date.now() - startTime);

    await redisClient.set(dedupKey, "1", "EX", DEDUP_TTL);

    logger.info({ event: event.event, projectId: event.projectId, traceId }, "Event published");
    return c.json({ status: "accepted", traceId }, 202);
  } finally {
    span.end();
  }
});

app.get("/health", (c) => c.json({ status: "ok" }));
app.get("/metrics", (_c) => metricsHandler());

const port = parseInt(process.env.PORT || "3004");

export default {
  port,
  fetch: app.fetch,
};
