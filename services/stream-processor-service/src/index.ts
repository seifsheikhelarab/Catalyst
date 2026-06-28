import { getProducer, createConsumer, sendToDLQ, TOPICS, type Consumer, type Producer } from "@catalyst/kafka";
import { connectRedis } from "@catalyst/redis";
import { type EnrichedEvent } from "@catalyst/types";
import { createLogger, flushLogs } from "@catalyst/logger";
import { initTracing, startSpanWithTraceContext, shutdownTracing } from "@catalyst/tracing";
import { createCounter, createHistogram, metricsHandler } from "@catalyst/metrics";
import type { RedisClient } from "@catalyst/redis";
import pg from "pg";

const { Pool } = pg;

const logger = createLogger({ name: "stream-processor-service" });

const eventsProcessed = createCounter({ name: "stream_events_processed_total", help: "Events processed for rollups" });
const eventsRetried = createCounter({ name: "stream_events_retried_total", help: "Events retried after transient failure" });
const eventsDLQd = createCounter({ name: "stream_events_dlqd_total", help: "Events sent to DLQ after retries" });
const rollupsFlushed = createCounter({ name: "stream_rollups_flushed_total", help: "Rollup batches flushed to TimescaleDB" });
const processingLag = createHistogram({ name: "stream_processing_lag_ms", help: "Event processing lag ms", buckets: [10, 50, 100, 250, 500, 1000, 5000] });

const MAX_RETRIES = 3;
const BASE_BACKOFF_MS = 200;

async function sleep(ms: number) {
  await new Promise((r) => setTimeout(r, ms));
}

const CONSUMER_GROUP = "stream-processor-service";
const INPUT_TOPIC = TOPICS.ENRICHED_EVENTS;
const HLL_TTL_SECONDS = 7 * 24 * 60 * 60;

let consumer: Consumer;
let producer: Producer;
let redis: RedisClient | null = null;
let pgPool: pg.Pool;
let rollupTimer: ReturnType<typeof setInterval> | null = null;
let metricsServer: ReturnType<typeof Bun.serve> | null = null;
let inFlight = 0;
let draining = false;
const ROLLUP_INTERVAL_MS = 60_000;

function bucketKey(projectId: string, event: string, bucketMs: number): string {
  const ts = Math.floor(Date.now() / bucketMs) * bucketMs;
  return `counter:${projectId}:${event}:${ts}`;
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

async function processEvent(event: EnrichedEvent) {
  const ts = event.timestamp || Date.now();
  const userId = event.userId || "";

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

async function scanKeys(pattern: string): Promise<string[]> {
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

  // First pass: collect all valid keys with their metadata and build pipelines
  const keyEntries: Array<{ key: string; projectId: string; event: string; ts: number; hllKey: string }> = [];

  for (const key of keys) {
    const parts = key.split(":");
    if (parts.length < 4) continue;
    const [prefix, projectId, event, tsStr] = parts;
    if (prefix !== "counter") continue;

    const ts = parseInt(tsStr, 10);
    if (isNaN(ts)) continue;
    if (ts > cutoff) continue;

    keyEntries.push({ key, projectId, event, ts, hllKey: hllKey(projectId, event, dateStr(ts)) });
  }

  if (keyEntries.length === 0) return;

  // Batch all GET+DEL operations
  const counterPipeline = redis.pipeline();
  for (const entry of keyEntries) {
    counterPipeline.get(entry.key);
    counterPipeline.del(entry.key);
  }
  const counterResults = await counterPipeline.exec();

  // Batch all PFCOUNT operations
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

  // Process results
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
    const projectId = parts[0];
    const event = parts[1];
    values.push([projectId, event, new Date(ts).toISOString(), count, unique]);
  }

  try {
    await pgPool.query(
      `
      INSERT INTO event_rollups (project_id, event, bucket, count, unique_users)
      VALUES ${values.map((_, i) => `($${i * 5 + 1}, $${i * 5 + 2}, $${i * 5 + 3}, $${i * 5 + 4}, $${i * 5 + 5})`).join(",")}
      ON CONFLICT (project_id, event, bucket) DO UPDATE SET count = event_rollups.count + EXCLUDED.count, unique_users = GREATEST(event_rollups.unique_users, EXCLUDED.unique_users)
    `,
      values.flat(),
    );
    logger.info({ count: rolls.size }, "Rollups written to TimescaleDB");
    rollupsFlushed.inc(rolls.size);

    for (const [key, { count, unique, ts }] of rolls) {
      const parts = key.split("|");
      redis.publish(`live:${parts[0]}`, JSON.stringify({
        type: "metric_update",
        projectId: parts[0],
        event: parts[1],
        count_1m: count,
        active_users: unique,
        timestamp: ts,
      })).catch((err: unknown) => { logger.error({ error: err }, "Failed to publish live update"); });
    }
  } catch (err) {
    logger.error({ error: err }, "Failed to write rollups");
  }
}

async function shutdown() {
  if (draining) return;
  draining = true;
  logger.info({ inFlight }, "Shutting down stream-processor service...");

  const deadline = Date.now() + 25_000;
  while (inFlight > 0 && Date.now() < deadline) {
    await sleep(100);
  }
  if (inFlight > 0) {
    logger.warn({ inFlight }, "Shutdown deadline reached with in-flight messages");
  }

  await flushRollups();
  await consumer?.stop();
  await consumer?.disconnect();
  await redis?.quit();
  await pgPool?.end();
  if (rollupTimer) clearInterval(rollupTimer);
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

  rollupTimer = setInterval(flushRollups, ROLLUP_INTERVAL_MS);

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
    port: parseInt(process.env.METRICS_PORT || "9104"),
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
