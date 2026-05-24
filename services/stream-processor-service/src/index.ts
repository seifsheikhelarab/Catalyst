import { getKafka, TOPICS, type Consumer } from "@catalyst/kafka";
import { connectRedis } from "@catalyst/redis";
import { type EnrichedEvent } from "@catalyst/types";
import { createLogger } from "@catalyst/logger";
import { initTracing } from "@catalyst/tracing";
import { createCounter, createHistogram, metricsHandler } from "@catalyst/metrics";
import pg from "pg";

const { Pool } = pg;

const logger = createLogger({ name: "stream-processor-service" });

const eventsProcessed = createCounter({ name: "stream_events_processed_total", help: "Events processed for rollups" });
const rollupsFlushed = createCounter({ name: "stream_rollups_flushed_total", help: "Rollup batches flushed to TimescaleDB" });
const processingLag = createHistogram({ name: "stream_processing_lag_ms", help: "Event processing lag ms", buckets: [10, 50, 100, 250, 500, 1000, 5000] });

const CONSUMER_GROUP = "stream-processor-service";
const INPUT_TOPIC = TOPICS.ENRICHED_EVENTS;
const HLL_TTL_SECONDS = 7 * 24 * 60 * 60;

const kafka = getKafka({ clientId: "stream-processor-service" });

let consumer: Consumer;
let redis: any;
let pgPool: pg.Pool;
let rollupTimer: any = null;
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

  for (const key of keys) {
    const parts = key.split(":");
    if (parts.length < 4) continue;
    const [prefix, projectId, event, tsStr] = parts;
    if (prefix !== "counter") continue;

    const ts = parseInt(tsStr, 10);
    if (isNaN(ts)) continue;
    if (ts > cutoff) continue;

    const pipeline = redis.pipeline();
    pipeline.get(key);
    pipeline.del(key);
    const results = await pipeline.exec();
    const countStr = results?.[0]?.[1] as string;
    const count = parseInt(countStr || "0", 10);
    if (count === 0) continue;

    const hllKeyVal = hllKey(projectId, event, dateStr(ts));
    const unique = parseInt((await redis.pfcount(hllKeyVal)) || "0", 10);

    const bucket = minuteBucket(ts);
    const mapKey = `${projectId}|${event}|${bucket}`;

    if (rolls.has(mapKey)) {
      rolls.get(mapKey)!.count += count;
    } else {
      rolls.set(mapKey, { count, unique, ts: bucket });
    }
  }

  if (rolls.size === 0) return;

  logger.info({ rollupCount: rolls.size }, "Flushing rollups to TimescaleDB");

  const values: any[][] = [];
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
      })).catch((err: any) => { logger.error({ error: err }, "Failed to publish live update"); });
    }
  } catch (err) {
    logger.error({ error: err }, "Failed to write rollups");
  }
}

async function shutdown() {
  logger.info("Shutting down...");
  await flushRollups();
  await consumer?.stop();
  await consumer?.disconnect();
  await redis?.quit();
  await pgPool?.end();
  if (rollupTimer) clearInterval(rollupTimer);
  process.exit(0);
}

process.on("SIGTERM", shutdown);
process.on("SIGINT", shutdown);

async function start() {
  initTracing({ serviceName: "stream-processor-service" });
  logger.info({ topic: INPUT_TOPIC, group: CONSUMER_GROUP }, "Starting stream-processor service");

  redis = await connectRedis();
  consumer = kafka.consumer({ groupId: CONSUMER_GROUP });
  await consumer.connect();

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
    eachMessage: async ({ message }) => {
      const startTime = Date.now();
      const rawValue = message.value?.toString();
      if (!rawValue) return;

      try {
        const event = JSON.parse(rawValue) as EnrichedEvent;
        await processEvent(event);
        eventsProcessed.inc();
        processingLag.observe(Date.now() - startTime);
      } catch (err) {
        logger.error({ error: err }, "Error processing event");
      }
    },
  });

  logger.info("Stream-processor service running");
}

start().catch((err) => {
  logger.error({ error: err }, "Fatal error");
  process.exit(1);
});
