import { Hono } from "hono";
import { getProducer, TOPICS } from "@catalyst/kafka";
import { connectRedis } from "@catalyst/redis";
import { RawEventSchema, type RawEvent } from "@catalyst/types";
import { createLogger } from "@catalyst/logger";
import crypto from "crypto";

const logger = createLogger({ name: "ingest-service" });

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

  const redisClient = await getRedisClient();
  const dedupKey = await computeDedupKey(event);

  const existing = await redisClient.get(dedupKey);
  if (existing) {
    logger.info({ dedupKey, event: event.event }, "Duplicate event rejected");
    return c.json({ status: "duplicate" }, 202);
  }

  const prod = await getKafkaProducer();
  const traceId = crypto.randomUUID();

  const message = {
    ...event,
    traceId,
  };

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

  await redisClient.set(dedupKey, "1", "EX", DEDUP_TTL);

  logger.info({ event: event.event, projectId: event.projectId, traceId }, "Event published");

  return c.json({ status: "accepted", traceId }, 202);
});

app.get("/health", (c) => c.json({ status: "ok" }));

const port = parseInt(process.env.PORT || "3004");

export default {
  port,
  fetch: app.fetch,
};
