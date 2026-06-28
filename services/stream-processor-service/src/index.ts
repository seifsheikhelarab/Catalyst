import { getProducer, createConsumer, sendToDLQ, TOPICS, type Consumer, type Producer } from "@catalyst/kafka";
import { connectRedis } from "@catalyst/redis";
import type { RedisClient } from "@catalyst/redis";
import { type EnrichedEvent } from "@catalyst/types";
import { createLogger, flushLogs, sleep } from "@catalyst/logger";
import { initTracing, startSpanWithTraceContext, shutdownTracing } from "@catalyst/tracing";
import { createCounter, createGauge, createHistogram, metricsHandler } from "@catalyst/metrics";
import { createBreaker } from "@catalyst/circuit-breaker";
import pg from "pg";

const { Pool } = pg;
const logger = createLogger({ name: "stream-processor-service" });

const eventsProcessed = createCounter({ name: "stream_events_processed_total", help: "Events processed for rollups" });
const eventsStored = createCounter({ name: "stream_events_stored_total", help: "Raw events stored in TimescaleDB" });
const eventsDropped = createCounter({ name: "stream_events_dropped_total", help: "Events dropped due to backpressure" });
const eventsRetried = createCounter({ name: "stream_events_retried_total", help: "Events retried after transient failure" });
const eventsDLQd = createCounter({ name: "stream_events_dlqd_total", help: "Events sent to DLQ after retries" });
const rollupsFlushed = createCounter({ name: "stream_rollups_flushed_total", help: "Rollup batches flushed to TimescaleDB" });
const bufferSize = createGauge({ name: "stream_buffer_size", help: "Current in-memory buffer size" });
const processingLag = createHistogram({ name: "stream_processing_lag_ms", help: "Event processing lag ms", buckets: [10, 50, 100, 250, 500, 1000, 5000] });
const insertDuration = createHistogram({ name: "stream_insert_duration_ms", help: "TimescaleDB insert duration ms", buckets: [50, 100, 250, 500, 1000, 2500, 5000] });

const MAX_RETRIES = 3;
const BASE_BACKOFF_MS = 200;

const CONSUMER_GROUP = "stream-processor-service";
const INPUT_TOPIC = TOPICS.ENRICHED_EVENTS;
const HLL_TTL_SECONDS = 7 * 24 * 60 * 60;
const BATCH_SIZE = 1000;
const FLUSH_INTERVAL_MS = 5000;
const MAX_BUFFER_SIZE = 50_000;

let consumer: Consumer;
let producer: Producer;
let redis: RedisClient | null = null;
let pgPool: pg.Pool;
let rollupTimer: ReturnType<typeof setInterval> | null = null;
let metricsServer: ReturnType<typeof Bun.serve> | null = null;
let inFlight = 0;
let draining = false;

let eventBuffer: EnrichedEvent[] = [];
let lastFlush = Date.now();
let flushTimer: ReturnType<typeof setTimeout> | null = null;

const ROLLUP_INTERVAL_MS = 60_000;

function bucketKey(projectId: string, event: string, bucketMs: number): string {
  return `counter:${projectId}:${event}:${Math.floor(Date.now() / bucketMs) * bucketMs}`;
}

function hllKey(projectId: string, event: string, date: string): string {
  return `hll:${projectId}:${event}:${date}`;
}

function dateStr(ts: number): string {
  return new Date(ts).toISOString().slice(0, 10);
}

function minuteBucket(ts: number): number {
  return Math.floor(ts / 60_000) * 60_000;
}

const insertBreaker = createBreaker(
  async (rows: EnrichedEvent[]) => {
    const values = rows.map((event) => [
      event.projectId,
      event.event,
      event.userId || "",
      event.sessionId || "",
      event.country || "",
      event.city || "",
      event.deviceType || "",
      event.browser || "",
      event.os || "",
      event.properties ? JSON.stringify(event.properties) : "{}",
      new Date(event.timestamp).toISOString(),
    ]);

    const placeholders = values.map((_, i) =>
      `($${i * 11 + 1}, $${i * 11 + 2}, $${i * 11 + 3}, $${i * 11 + 4}, $${i * 11 + 5}, $${i * 11 + 6}, $${i * 11 + 7}, $${i * 11 + 8}, $${i * 11 + 9}, $${i * 11 + 10}, $${i * 11 + 11})`
    ).join(",");

    const end = insertDuration.startTimer();
    await pgPool.query(
      `INSERT INTO events (project_id, event, user_id, session_id, country, city, device_type, browser, os, properties, timestamp) VALUES ${placeholders}`,
      values.flat(),
    );
    end();
  },
  {
    name: "timescaledb-insert",
    timeout: 10_000,
    errorThresholdPercentage: 50,
    resetTimeout: 15_000,
    volumeThreshold: 3,
    fallback: () => {
      logger.warn({ bufferSize: eventBuffer.length }, "TimescaleDB circuit open");
      throw new Error("TimescaleDB circuit open");
    },
  },
);

async function processEvent(event: EnrichedEvent) {
  const ts = event.timestamp || Date.now();
  const userId = event.userId || "";
  if (!redis) throw new Error("Redis not connected");

  await Promise.all([
    redis.incr(bucketKey(event.projectId, event.event, 60_000)),
    redis.incr(bucketKey(event.projectId, event.event, 3_600_000)),
    redis.incr(bucketKey(event.projectId, event.event, 86_400_000)),
  ]);

  if (userId) {
    const key = hllKey(event.projectId, event.event, dateStr(ts));
    await redis.pfadd(key, userId);
    await redis.expire(key, HLL_TTL_SECONDS);
  }
}

async function flushEventBuffer() {
  if (eventBuffer.length === 0) return;
  if (insertBreaker.isOpen) return;

  const rows = eventBuffer.splice(0, BATCH_SIZE);
  if (rows.length === 0) return;

  const now = Date.now();
  logger.info({ count: rows.length }, "Flushing batch to TimescaleDB");

  try {
    await insertBreaker.fire(rows);
    eventsStored.inc(rows.length);
    logger.info({ count: rows.length, ms: Date.now() - now }, "Batch inserted");
    lastFlush = Date.now();
  } catch (err) {
    logger.warn({ error: err, count: rows.length }, "TimescaleDB insert failed");
    const overflow = eventBuffer.length + rows.length - MAX_BUFFER_SIZE;
    if (overflow > 0) {
      const dropped = eventBuffer.splice(0, overflow);
      eventsDropped.inc(dropped.length);
      logger.error({ dropped: dropped.length }, "Buffer overflow, dropped oldest events");
    }
    eventBuffer.unshift(...rows);
  }
}

function scheduleEventFlush() {
  if (flushTimer) clearTimeout(flushTimer);
  const elapsed = Date.now() - lastFlush;
  flushTimer = setTimeout(flushEventBuffer, Math.max(0, FLUSH_INTERVAL_MS - elapsed));
}

async function scanKeys(pattern: string): Promise<string[]> {
  if (!redis) throw new Error("Redis not connected");
  const keys: string[] = [];
  let cursor = "0";
  do {
    const [nextCursor, batch] = await redis.scan(cursor, "MATCH", pattern, "COUNT", 100);
    cursor = nextCursor;
    keys.push(...batch);
  } while (cursor !== "0");
  return keys;
}

async function flushRollups() {
  const now = Date.now();
  const cutoff = now - ROLLUP_INTERVAL_MS * 2;

  const keys = await scanKeys("counter:*");
  if (keys.length === 0) return;

  const rolls: Map<string, { count: number; unique: number; ts: number }> = new Map();

  const keyEntries: Array<{ key: string; projectId: string; event: string; ts: number; hllKey: string }> = [];

  for (const key of keys) {
    const parts = key.split(":");
    if (parts.length < 4) continue;
    const [, projectId, event, tsStr] = parts;
    const ts = parseInt(tsStr, 10);
    if (isNaN(ts)) continue;
    if (ts > cutoff) continue;
    keyEntries.push({ key, projectId, event, ts, hllKey: hllKey(projectId, event, dateStr(ts)) });
  }

  if (keyEntries.length === 0) return;

  if (!redis) throw new Error("Redis not connected");

  const counterPipeline = redis.pipeline();
  for (const entry of keyEntries) {
    counterPipeline.get(entry.key);
    counterPipeline.del(entry.key);
  }
  const counterResults = await counterPipeline.exec();

  if (!redis) throw new Error("Redis not connected");

  const hllPipeline = redis.pipeline();
  const hllKeyMap = new Map<string, number>();
  for (let i = 0; i < keyEntries.length; i++) {
    const hk = keyEntries[i].hllKey;
    if (!hllKeyMap.has(hk)) {
      hllKeyMap.set(hk, hllKeyMap.size);
      hllPipeline.pfcount(hk);
    }
  }
  const hllResults = await hllPipeline.exec();

  const hllValues = new Map<string, number>();
  for (const [hk, idx] of hllKeyMap) {
    const result = hllResults?.[idx];
    const err = result?.[0];
    const val = result?.[1];
    if (err) {
      logger.warn({ error: err, key: hk }, "HLL PFCOUNT failed");
      continue;
    }
    hllValues.set(hk, parseInt(String(val || "0"), 10));
  }

  for (let i = 0; i < keyEntries.length; i++) {
    const entry = keyEntries[i];
    const counterResult = counterResults?.[i * 2];
    const counterErr = counterResult?.[0];
    const countStr = counterResult?.[1] as string | undefined;

    if (counterErr) {
      logger.warn({ error: counterErr, key: entry.key }, "Counter GET/DEL failed");
      continue;
    }

    const count = parseInt(countStr || "0", 10);
    if (count === 0) continue;

    const unique = hllValues.get(entry.hllKey) || 0;
    const bucket = minuteBucket(entry.ts);
    const mapKey = `${entry.projectId}|${entry.event}|${bucket}`;

    if (rolls.has(mapKey)) {
      rolls.get(mapKey)!.count += count;
    } else {
      rolls.set(mapKey, { count, unique, ts: bucket });
    }
  }

  if (rolls.size === 0) return;

  logger.info({ rollupCount: rolls.size }, "Flushing rollups to TimescaleDB");

  const values: Array<Array<string | number>> = [];
  for (const [key, { count, unique, ts }] of rolls) {
    const parts = key.split("|");
    values.push([parts[0], parts[1], new Date(ts).toISOString(), count, unique]);
  }

  try {
    await pgPool.query(
      `INSERT INTO event_rollups (project_id, event, bucket, count, unique_users) VALUES ${values.map((_, i) => `($${i * 5 + 1}, $${i * 5 + 2}, $${i * 5 + 3}, $${i * 5 + 4}, $${i * 5 + 5})`).join(",")} ON CONFLICT (project_id, event, bucket) DO UPDATE SET count = event_rollups.count + EXCLUDED.count, unique_users = GREATEST(event_rollups.unique_users, EXCLUDED.unique_users)`,
      values.flat(),
    );
    logger.info({ count: rolls.size }, "Rollups written to TimescaleDB");
    rollupsFlushed.inc(rolls.size);

    for (const [key, { count, unique, ts }] of rolls) {
      const parts = key.split("|");
      const publishKey = `live:${parts[0]}`;
      redis?.publish(
        publishKey,
        JSON.stringify({ type: "metric_update", projectId: parts[0], event: parts[1], count_1m: count, active_users: unique, timestamp: ts }),
      ).catch((err: unknown) => { logger.error({ error: err }, "Failed to publish live update"); });
    }
  } catch (err) {
    logger.error({ error: err }, "Failed to write rollups");
  }
}

async function shutdown() {
  if (draining) return;
  draining = true;
  logger.info({ inFlight, bufferSize: eventBuffer.length }, "Shutting down stream-processor service...");

  const deadline = Date.now() + 25_000;
  while (inFlight > 0 && Date.now() < deadline) await sleep(100);
  if (inFlight > 0) logger.warn({ inFlight }, "Shutdown deadline reached with in-flight messages");

  await flushEventBuffer();
  await flushRollups();
  await consumer?.stop();
  await consumer?.disconnect();
  await redis?.quit();
  await pgPool?.end();
  if (rollupTimer) clearInterval(rollupTimer);
  if (flushTimer) clearTimeout(flushTimer);
  insertBreaker.shutdown();
  metricsServer?.stop();
  await shutdownTracing();
  await flushLogs();
  process.exit(0);
}

process.on("SIGTERM", shutdown);
process.on("SIGINT", shutdown);

async function start() {
  initTracing({ serviceName: "stream-processor-service" });
  logger.info({ topic: INPUT_TOPIC, group: CONSUMER_GROUP }, "Starting stream-processor service");

  redis = await connectRedis();
  consumer = await createConsumer(CONSUMER_GROUP);
  producer = await getProducer();

  pgPool = new Pool({
    host: process.env.POSTGRES_HOST || "localhost",
    port: parseInt(process.env.POSTGRES_PORT || "5432"),
    user: process.env.POSTGRES_USER || "catalyst",
    password: process.env.POSTGRES_PASSWORD || "catalyst",
    database: process.env.POSTGRES_DB || "catalyst",
    max: 10,
  });

  rollupTimer = setInterval(() => { flushRollups().catch((err) => logger.error({ error: err }, "Rollup flush failed")); }, ROLLUP_INTERVAL_MS);

  setInterval(() => {
    bufferSize.set(eventBuffer.length);
  }, 1000).unref();

  await consumer.subscribe({ topic: INPUT_TOPIC, fromBeginning: false });

  await consumer.run({
    autoCommit: false,
    eachBatch: async ({ batch, resolveOffset, commitOffsetsIfNecessary, heartbeat, isRunning, isStale }) => {
      for (const message of batch.messages) {
        if (!isRunning() || isStale()) break;
        if (!message.value) { resolveOffset(message.offset); continue; }

        inFlight++;
        const startTime = Date.now();
        const traceparent = message.headers?.traceparent?.toString();
        const span = startSpanWithTraceContext({ traceparent }, "stream-processor.process");

        try {
          const event = JSON.parse(message.value.toString()) as EnrichedEvent;
          span.setAttribute("event.projectId", event.projectId);
          span.setAttribute("event.type", event.event);

          let lastErr: unknown;
          for (let attempt = 1; attempt <= MAX_RETRIES; attempt++) {
            try {
              await processEvent(event);
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
            logger.error({ error: lastErr }, "Sent to DLQ after max retries");
            resolveOffset(message.offset);
            await heartbeat();
            continue;
          }

          // Buffer for raw event storage
          if (eventBuffer.length >= MAX_BUFFER_SIZE) {
            eventBuffer.shift();
            eventsDropped.inc();
            logger.warn({ maxBuffer: MAX_BUFFER_SIZE }, "Buffer at max, dropping oldest event");
          }
          eventBuffer.push(event);
          if (eventBuffer.length >= BATCH_SIZE) {
            if (flushTimer) clearTimeout(flushTimer);
            await flushEventBuffer();
          } else {
            scheduleEventFlush();
          }

          eventsProcessed.inc();
          processingLag.observe(Date.now() - startTime);
          resolveOffset(message.offset);
          await heartbeat();
        } catch (err) {
          logger.error({ error: err, offset: message.offset }, "Unrecoverable error, sending to DLQ");
          try {
            await sendToDLQ(producer, { topic: batch.topic, partition: batch.partition, message }, err);
            eventsDLQd.inc();
          } catch (dlqErr) {
            logger.error({ error: dlqErr }, "Failed to send to DLQ");
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
    port: parseInt(process.env.METRICS_PORT || "9103"),
    fetch(req) {
      const url = new URL(req.url);
      if (url.pathname === "/metrics") return metricsHandler();
      if (url.pathname === "/health") return Response.json({ status: "ok" });
      return new Response("not found", { status: 404 });
    },
  });

  logger.info("Stream-processor service running");
}

start().catch((err) => {
  logger.error({ error: err }, "Fatal error");
  process.exit(1);
});
