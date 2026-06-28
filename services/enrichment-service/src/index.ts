import { createConsumer, getProducer, sendToDLQ, TOPICS, type Consumer, type Producer } from "@catalyst/kafka";
import { connectRedis } from "@catalyst/redis";
import type Redis from "ioredis";
import { type EnrichedEvent, type ValidatedEvent } from "@catalyst/types";
import { createLogger, flushLogs } from "@catalyst/logger";
import { initTracing, startSpanWithTraceContext, injectTraceHeaders, shutdownTracing } from "@catalyst/tracing";
import { createCounter, createHistogram, metricsHandler } from "@catalyst/metrics";
import crypto from "crypto";
import { UAParser } from "ua-parser-js";
import geoip from "geoip-lite";

const logger = createLogger({ name: "enrichment-service" });

const eventsProcessed = createCounter({ name: "enrichment_events_processed_total", help: "Events enriched and forwarded" });
const eventsRetried = createCounter({ name: "enrichment_events_retried_total", help: "Events retried after transient failure" });
const eventsDLQd = createCounter({ name: "enrichment_events_dlqd_total", help: "Events sent to DLQ after retries" });
const processingDuration = createHistogram({
  name: "enrichment_processing_duration_ms",
  help: "Enrichment processing time",
  buckets: [5, 10, 25, 50, 100, 250, 500, 1000],
});

const CONSUMER_GROUP = "enrichment-service";
const INPUT_TOPIC = TOPICS.VALIDATED_EVENTS;
const OUTPUT_TOPIC = TOPICS.ENRICHED_EVENTS;
const SESSION_TTL = 1800;
const MAX_RETRIES = 3;
const BASE_BACKOFF_MS = 200;

let consumer: Consumer;
let producer: Producer;
let redis: Redis | null = null;

let inFlight = 0;
let draining = false;
let metricsServer: ReturnType<typeof Bun.serve> | null = null;

async function sleep(ms: number) {
  await new Promise((r) => setTimeout(r, ms));
}

async function shutdown() {
  if (draining) return;
  draining = true;
  logger.info({ inFlight }, "Shutting down enrichment service...");

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

async function getOrCreateSession(projectId: string, userId: string): Promise<string> {
  const sessionKey = `session:${projectId}:${userId}`;
  if (!redis) return crypto.randomUUID();
  const existingSession = await redis.get(sessionKey);
  if (existingSession) {
    return existingSession;
  }

  const newSessionId = crypto.randomUUID();
  await redis.set(sessionKey, newSessionId, "EX", SESSION_TTL);
  return newSessionId;
}

function parseUserAgent(userAgent?: string) {
  if (!userAgent) {
    return { browser: "unknown", deviceType: "unknown", os: "unknown" };
  }
  const parser = new UAParser(userAgent);
  const ua = parser.getResult();
  return {
    browser: ua.browser.name || "unknown",
    deviceType: ua.device.type || "desktop",
    os: ua.os.name || "unknown",
  };
}

async function start() {
  initTracing({ serviceName: "enrichment-service" });
  logger.info({ topic: INPUT_TOPIC, group: CONSUMER_GROUP }, "Starting enrichment service");

  redis = await connectRedis() as unknown as Redis;
  consumer = await createConsumer(CONSUMER_GROUP);

  producer = await getProducer();

  await consumer.subscribe({ topic: INPUT_TOPIC, fromBeginning: false });

  await consumer.run({
    autoCommit: false,
    eachBatch: async ({ batch, resolveOffset, commitOffsetsIfNecessary, heartbeat, isRunning, isStale }) => {
      for (const message of batch.messages) {
        if (!isRunning() || isStale()) break;
        if (!message.value) {
          resolveOffset(message.offset);
          continue;
        }

        inFlight++;
        const traceId = message.headers?.traceId?.toString() || "unknown";
        const traceparent = message.headers?.traceparent?.toString();
        const span = startSpanWithTraceContext({ traceparent }, "enrichment.process");
        const start = Date.now();

        try {
          const validatedEvent = JSON.parse(message.value.toString()) as ValidatedEvent;
          span.setAttribute("event.projectId", validatedEvent.projectId);

          const { deviceType, browser, os } = parseUserAgent(
            validatedEvent.properties?.userAgent as string,
          );

          const ip =
            (validatedEvent.properties?.ip as string) ||
            (validatedEvent.properties?.clientIp as string);
          let country: string | undefined;
          let city: string | undefined;
          if (ip) {
            const geo = geoip.lookup(ip);
            if (geo) {
              country = geo.country;
              city = geo.city;
            }
          }

          const sessionId = validatedEvent.userId
            ? await getOrCreateSession(validatedEvent.projectId, validatedEvent.userId)
            : undefined;

          const enrichedEvent: EnrichedEvent = {
            ...validatedEvent,
            country,
            city,
            deviceType,
            browser,
            os,
            sessionId,
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
              if (attempt < MAX_RETRIES) {
                eventsRetried.inc();
                await sleep(BASE_BACKOFF_MS * Math.pow(2, attempt - 1));
              }
            }
          }

          if (lastErr) {
            await sendToDLQ(producer, { topic: batch.topic, partition: batch.partition, message }, lastErr);
            eventsDLQd.inc();
            logger.error({ traceId, error: lastErr }, "Sent to DLQ after max retries");
            resolveOffset(message.offset);
            await heartbeat();
            continue;
          }

          eventsProcessed.inc();
          processingDuration.observe(Date.now() - start);
          logger.info({ traceId, event: enrichedEvent.event, sessionId }, "Event enriched");
          resolveOffset(message.offset);
          await heartbeat();
        } catch (err) {
          logger.error({ traceId, error: err }, "Unrecoverable error, sending to DLQ");
          try {
            await sendToDLQ(producer, { topic: batch.topic, partition: batch.partition, message }, err);
            eventsDLQd.inc();
          } catch (dlqErr) {
            logger.error({ traceId, error: dlqErr }, "Failed to send to DLQ");
          }
          resolveOffset(message.offset);
        } finally {
          span.end();
          inFlight--;
        }
      }
      await commitOffsetsIfNecessary();
    },
  });

  metricsServer = Bun.serve({
    port: parseInt(process.env.METRICS_PORT || "9102"),
    fetch(req) {
      const url = new URL(req.url);
      if (url.pathname === "/metrics") return metricsHandler();
      if (url.pathname === "/health") return Response.json({ status: "ok" });
      return new Response("not found", { status: 404 });
    },
  });

  logger.info("Enrichment service running");
}

start().catch((err) => {
  logger.error({ error: err }, "Fatal error");
  process.exit(1);
});
