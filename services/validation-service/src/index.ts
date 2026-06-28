import { getKafka, getProducer, createConsumer, sendToDLQ, TOPICS, type Consumer, type Producer } from "@catalyst/kafka";
import { RawEventSchema, type RawEvent, type ValidatedEvent } from "@catalyst/types";
import { createLogger, flushLogs } from "@catalyst/logger";
import { connectRedis } from "@catalyst/redis";
import { initTracing, startSpanWithTraceContext, injectTraceHeaders, shutdownTracing } from "@catalyst/tracing";
import { createCounter, createHistogram, metricsHandler } from "@catalyst/metrics";
import type { KafkaMessage } from "kafkajs";
import type { RedisClient } from "@catalyst/redis";
import crypto from "crypto";

const logger = createLogger({ name: "validation-service" });

const eventsValid = createCounter({ name: "validation_events_valid_total", help: "Valid events forwarded" });
const eventsRejected = createCounter({ name: "validation_events_rejected_total", help: "Events rejected by schema" });
const eventsKafkaFailed = createCounter({ name: "validation_events_kafka_failed_total", help: "Events failed Kafka produce after retries" });
const processingDuration = createHistogram({
  name: "validation_processing_duration_ms",
  help: "Validation processing time",
  buckets: [5, 10, 25, 50, 100, 250, 500, 1000],
});

const CONSUMER_GROUP = "validation-service";
const INPUT_TOPIC = TOPICS.RAW_EVENTS;
const VALIDATED_TOPIC = TOPICS.VALIDATED_EVENTS;

const MAX_RETRIES = 3;
const BASE_BACKOFF_MS = 200;

const kafka = getKafka({ clientId: "validation-service" });

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

async function getProjectSchema(projectId: string, eventName: string): Promise<Record<string, unknown> | null> {
  try {
    const rclient = await getRedisClient();
    const cacheKey = `schema:${projectId}:${eventName}`;
    const cached = await rclient.get(cacheKey);
    if (cached) {
      const parsed = JSON.parse(cached);
      return parsed.schema || null;
    }

    // Cache miss — fetch from project-service
    const projectServiceUrl = process.env.PROJECT_SERVICE_URL || "http://localhost:3001";
    const res = await fetch(`${projectServiceUrl}/projects/${projectId}/schemas/${eventName}`);
    if (!res.ok) return null;
    const data = await res.json() as Record<string, unknown>;
    if (data && data.schema) {
      // Cache for 5 minutes
      await rclient.setex(cacheKey, 300, JSON.stringify(data));
      return data.schema as Record<string, unknown>;
    }
    return null;
  } catch {
    return null;
  }
}

function validateAgainstProjectSchema(schema: Record<string, unknown>, properties: Record<string, unknown> | undefined): string[] {
  const errors: string[] = [];
  const schemaObj = schema as Record<string, unknown>;

  // Check required fields exist
  const required = schemaObj.required;
  if (Array.isArray(required)) {
    for (const field of required) {
      if (properties == null || !(field in properties)) {
        errors.push(`Missing required property: ${String(field)}`);
      }
    }
  }

  // Check property types if defined
  const schemaProps = schemaObj.properties;
  if (schemaProps && typeof schemaProps === "object") {
    for (const [field, fieldSchema] of Object.entries(schemaProps as Record<string, Record<string, unknown>>)) {
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

async function sleep(ms: number) {
  await new Promise((r) => setTimeout(r, ms));
}

async function shutdown() {
  if (draining) return;
  draining = true;
  logger.info({ inFlight }, "Shutting down validation service...");

  const deadline = Date.now() + 25_000;
  while (inFlight > 0 && Date.now() < deadline) {
    await sleep(100);
  }
  if (inFlight > 0) {
    logger.warn({ inFlight }, "Shutdown deadline reached with in-flight messages");
  }

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

async function processMessageWithRetry(payload: Payload): Promise<void> {
  const { message, topic, partition } = payload;
  const rawValue = message.value?.toString();
  if (!rawValue) return;

  const traceId = message.headers?.traceId?.toString() || crypto.randomUUID();
  const traceparent = message.headers?.traceparent?.toString();

  const span = startSpanWithTraceContext({ traceparent }, "validation.process");
  const start = Date.now();

  try {
    const rawEvent = JSON.parse(rawValue) as RawEvent;
    span.setAttribute("event.projectId", rawEvent?.projectId ?? "unknown");

    const outHeaders = injectTraceHeaders({ traceId });

    const validationResult = RawEventSchema.safeParse(rawEvent);

    if (!validationResult.success) {
      logger.warn(
        { traceId, errors: validationResult.error.issues },
        "Event validation failed",
      );
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
        logger.warn(
          { traceId, errors: projectErrors },
          "Per-project schema validation failed",
        );
        await sendToDLQ(
          producer,
          { topic, partition, message },
          new Error(`Project schema validation failed: ${JSON.stringify(projectErrors)}`),
        );
        eventsRejected.inc();
        return;
      }
    }

    const validatedEvent: ValidatedEvent = {
      ...rawEvent,
      validatedAt: Date.now(),
    };

    let lastErr: unknown;
    for (let attempt = 1; attempt <= MAX_RETRIES; attempt++) {
      try {
        await producer.send({
          topic: VALIDATED_TOPIC,
          messages: [
            {
              key: validatedEvent.projectId,
              value: JSON.stringify(validatedEvent),
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
      eventsKafkaFailed.inc();
      return;
    }

    eventsValid.inc();
    processingDuration.observe(Date.now() - start);
    logger.info({ traceId, event: validatedEvent.event }, "Event validated");
  } finally {
    span.end();
  }
}

async function start() {
  initTracing({ serviceName: "validation-service" });
  logger.info({ topic: INPUT_TOPIC, group: CONSUMER_GROUP }, "Starting validation service");

  consumer = await createConsumer(CONSUMER_GROUP);
  producer = await getProducer();

  await consumer.subscribe({ topic: INPUT_TOPIC, fromBeginning: false });

  await consumer.run({
    autoCommit: false,
    eachBatch: async ({ batch, resolveOffset, commitOffsetsIfNecessary, heartbeat, isRunning, isStale }) => {
      for (const message of batch.messages) {
        if (!isRunning() || isStale()) break;
        inFlight++;
        try {
          await processMessageWithRetry({ topic: batch.topic, partition: batch.partition, message });
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

  logger.info("Validation service running");
}

start().catch((err) => {
  logger.error({ error: err }, "Fatal error");
  process.exit(1);
});
