import {
  getKafka,
  getProducer,
  createConsumer,
  sendToDLQ,
  TOPICS,
  type Consumer,
  type Producer,
} from "@catalyst/kafka";
import { RawEventSchema, type RawEvent, type EnrichedEvent } from "@catalyst/types";
import { createLogger, flushLogs, sleep } from "@catalyst/logger";
import { connectRedis } from "@catalyst/redis";
import type { RedisClient } from "@catalyst/redis";
import {
  initTracing,
  startSpanWithTraceContext,
  injectTraceHeaders,
  shutdownTracing,
} from "@catalyst/tracing";
import { createCounter, createHistogram, metricsHandler } from "@catalyst/metrics";
import type { KafkaMessage } from "@catalyst/kafka";
import crypto from "crypto";
import { UAParser } from "ua-parser-js";
import geoip from "geoip-lite";

const logger = createLogger({ name: "validate-enrich-service" });

const eventsValid = createCounter({
  name: "ve_events_valid_total",
  help: "Events validated and enriched",
});
const eventsRejected = createCounter({
  name: "ve_events_rejected_total",
  help: "Events rejected by schema",
});
const eventsDLQd = createCounter({ name: "ve_events_dlqd_total", help: "Events sent to DLQ" });
const processingDuration = createHistogram({
  name: "ve_processing_duration_ms",
  help: "Validation+enrichment processing time",
  buckets: [5, 10, 25, 50, 100, 250, 500, 1000],
});

const CONSUMER_GROUP = "validate-enrich-service";
const INPUT_TOPIC = TOPICS.RAW_EVENTS;
const OUTPUT_TOPIC = TOPICS.ENRICHED_EVENTS;
const SESSION_TTL = 1800;
const MAX_RETRIES = 3;
const BASE_BACKOFF_MS = 200;

const kafka = getKafka({ clientId: "validate-enrich-service" });

let consumer: Consumer;
let producer: Producer;
let redis: RedisClient | null = null;
let inFlight = 0;
let draining = false;
let metricsServer: ReturnType<typeof Bun.serve> | null = null;

async function getRedisClient() {
  if (!redis) redis = await connectRedis();
  return redis;
}

async function getProjectSchema(
  projectId: string,
  eventName: string,
): Promise<Record<string, unknown> | null> {
  try {
    const rclient = await getRedisClient();
    const cacheKey = `schema:${projectId}:${eventName}`;
    const cached = await rclient.get(cacheKey);
    if (cached) {
      const parsed = JSON.parse(cached);
      return parsed.schema || null;
    }
    const managementUrl = process.env.MANAGEMENT_SERVICE_URL || "http://localhost:3001";
    const res = await fetch(`${managementUrl}/projects/${projectId}/schemas/${eventName}`);
    if (!res.ok) return null;
    const data = (await res.json()) as Record<string, unknown>;
    if (data && data.schema) {
      await rclient.setex(cacheKey, 300, JSON.stringify(data));
      return data.schema as Record<string, unknown>;
    }
    return null;
  } catch {
    return null;
  }
}

function validateAgainstProjectSchema(
  schema: Record<string, unknown>,
  properties: Record<string, unknown> | undefined,
): string[] {
  const errors: string[] = [];
  const schemaObj = schema as Record<string, unknown>;
  const required = schemaObj.required;
  if (Array.isArray(required)) {
    for (const field of required) {
      if (properties == null || !(field in properties)) {
        errors.push(`Missing required property: ${String(field)}`);
      }
    }
  }
  const schemaProps = schemaObj.properties;
  if (schemaProps && typeof schemaProps === "object") {
    for (const [field, fieldSchema] of Object.entries(
      schemaProps as Record<string, Record<string, unknown>>,
    )) {
      if (properties && field in properties && fieldSchema.type) {
        const value = properties[field];
        switch (fieldSchema.type) {
          case "string":
            if (typeof value !== "string") errors.push(`Field ${field} should be a string`);
            break;
          case "number":
          case "integer":
            if (typeof value !== "number") errors.push(`Field ${field} should be a number`);
            break;
          case "boolean":
            if (typeof value !== "boolean") errors.push(`Field ${field} should be a boolean`);
            break;
        }
      }
    }
  }
  return errors;
}

async function getOrCreateSession(projectId: string, userId: string): Promise<string> {
  if (!redis) return crypto.randomUUID();
  const sessionKey = `session:${projectId}:${userId}`;
  const existingSession = await redis.get(sessionKey);
  if (existingSession) return existingSession;
  const newSessionId = crypto.randomUUID();
  await redis.set(sessionKey, newSessionId, "EX", SESSION_TTL);
  return newSessionId;
}

function parseUserAgent(userAgent?: string) {
  if (!userAgent) return { browser: "unknown", deviceType: "unknown", os: "unknown" };
  const parser = new UAParser(userAgent);
  const ua = parser.getResult();
  return {
    browser: ua.browser.name || "unknown",
    deviceType: ua.device.type || "desktop",
    os: ua.os.name || "unknown",
  };
}

async function shutdown() {
  if (draining) return;
  draining = true;
  logger.info({ inFlight }, "Shutting down validate-enrich service...");
  const deadline = Date.now() + 25_000;
  while (inFlight > 0 && Date.now() < deadline) await sleep(100);
  if (inFlight > 0) logger.warn({ inFlight }, "Shutdown deadline reached with in-flight messages");
  await consumer?.stop();
  await consumer?.disconnect();
  await producer?.disconnect();
  await redis?.quit();
  metricsServer?.stop();
  await shutdownTracing();
  await flushLogs();
  process.exit(0);
}

process.on("SIGTERM", shutdown);
process.on("SIGINT", shutdown);

interface Payload {
  topic: string;
  partition: number;
  message: KafkaMessage;
}

function extractProperties(raw: Record<string, unknown>): {
  ip: string | undefined;
  userAgent: string | undefined;
  userId: string | undefined;
} {
  const props = (raw.properties ?? {}) as Record<string, unknown>;
  return {
    ip: (props.ip as string | undefined) || (props.clientIp as string | undefined),
    userAgent: props.userAgent as string | undefined,
    userId: raw.userId as string | undefined,
  };
}

async function processEvent(payload: Payload): Promise<void> {
  const { message, topic, partition } = payload;
  const rawValue = message.value?.toString();
  if (!rawValue) return;

  const traceId = message.headers?.traceId?.toString() || crypto.randomUUID();
  const traceparent = message.headers?.traceparent?.toString();

  const span = startSpanWithTraceContext({ traceparent }, "validate-enrich.process");
  const start = Date.now();

  try {
    const rawEvent = JSON.parse(rawValue) as RawEvent;
    span.setAttribute("event.projectId", rawEvent.projectId ?? "unknown");

    const { ip, userAgent, userId } = extractProperties(
      rawEvent as unknown as Record<string, unknown>,
    );

    let country: string | undefined;
    let city: string | undefined;
    if (ip) {
      const geo = geoip.lookup(ip);
      if (geo) {
        country = geo.country;
        city = geo.city;
      }
    }

    const { deviceType, browser, os } = parseUserAgent(userAgent);

    const validationResult = RawEventSchema.safeParse(rawEvent);
    if (!validationResult.success) {
      logger.warn({ traceId, errors: validationResult.error.issues }, "Event validation failed");
      await sendToDLQ(
        producer,
        { topic, partition, message },
        new Error(`Schema validation failed: ${JSON.stringify(validationResult.error.issues)}`),
      );
      eventsRejected.inc();
      return;
    }

    // Per-project schema validation
    const projectSchema = await getProjectSchema(rawEvent.projectId, rawEvent.event);
    if (projectSchema) {
      const projectErrors = validateAgainstProjectSchema(projectSchema, rawEvent.properties);
      if (projectErrors.length > 0) {
        logger.warn({ traceId, errors: projectErrors }, "Per-project schema validation failed");
        await sendToDLQ(
          producer,
          { topic, partition, message },
          new Error(`Project schema validation failed: ${JSON.stringify(projectErrors)}`),
        );
        eventsRejected.inc();
        return;
      }
    }

    const sessionId = userId ? await getOrCreateSession(rawEvent.projectId, userId) : undefined;

    const enrichedEvent: EnrichedEvent = {
      ...rawEvent,
      country,
      city,
      deviceType,
      browser,
      os,
      sessionId,
      validatedAt: Date.now(),
      enrichedAt: Date.now(),
    };

    const outHeaders = injectTraceHeaders({ traceId });

    let lastErr: unknown;
    for (let attempt = 1; attempt <= MAX_RETRIES; attempt++) {
      try {
        await producer.send({
          topic: OUTPUT_TOPIC,
          messages: [
            {
              key: enrichedEvent.projectId,
              value: JSON.stringify(enrichedEvent),
              headers: Object.fromEntries(
                Object.entries(outHeaders).map(([k, v]) => [k, Buffer.from(v)]),
              ),
            },
          ],
        });
        lastErr = null;
        break;
      } catch (err) {
        lastErr = err;
        if (attempt < MAX_RETRIES) await sleep(BASE_BACKOFF_MS * Math.pow(2, attempt - 1));
      }
    }

    if (lastErr) {
      await sendToDLQ(producer, { topic, partition, message }, lastErr);
      eventsDLQd.inc();
      return;
    }

    eventsValid.inc();
    processingDuration.observe(Date.now() - start);
    logger.info({ traceId, event: enrichedEvent.event }, "Event validated and enriched");
  } finally {
    span.end();
  }
}

async function start() {
  initTracing({ serviceName: "validate-enrich-service" });
  logger.info({ topic: INPUT_TOPIC, group: CONSUMER_GROUP }, "Starting validate-enrich service");

  consumer = await createConsumer(CONSUMER_GROUP);
  producer = await getProducer();
  await getRedisClient();

  await consumer.subscribe({ topic: INPUT_TOPIC, fromBeginning: false });

  await consumer.run({
    autoCommit: false,
    eachBatch: async ({
      batch,
      resolveOffset,
      commitOffsetsIfNecessary,
      heartbeat,
      isRunning,
      isStale,
    }) => {
      for (const message of batch.messages) {
        if (!isRunning() || isStale()) break;
        inFlight++;
        try {
          await processEvent({ topic: batch.topic, partition: batch.partition, message });
          resolveOffset(message.offset);
          await heartbeat();
        } catch (err) {
          logger.error({ error: err, offset: message.offset }, "Unrecoverable error, skipping");
          resolveOffset(message.offset);
        } finally {
          inFlight--;
        }
      }
      await commitOffsetsIfNecessary();
    },
  });

  metricsServer = Bun.serve({
    port: parseInt(process.env.METRICS_PORT || "9101"),
    fetch(req) {
      const url = new URL(req.url);
      if (url.pathname === "/metrics") return metricsHandler();
      if (url.pathname === "/health") return Response.json({ status: "ok" });
      return new Response("not found", { status: 404 });
    },
  });

  logger.info("Validate-enrich service running");
}

start().catch((err) => {
  logger.error({ error: err }, "Fatal error");
  process.exit(1);
});
