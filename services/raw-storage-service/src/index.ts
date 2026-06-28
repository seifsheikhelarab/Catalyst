import { getKafka, createConsumer, TOPICS, type Consumer } from "@catalyst/kafka";
import { type EnrichedEvent } from "@catalyst/types";
import { createLogger, flushLogs } from "@catalyst/logger";
import { initTracing, startSpanWithTraceContext, shutdownTracing } from "@catalyst/tracing";
import { createCounter, createGauge, createHistogram, metricsHandler } from "@catalyst/metrics";
import { createBreaker, type TrackedBreaker } from "@catalyst/circuit-breaker";
import { ClickHouse } from "clickhouse";

const logger = createLogger({ name: "raw-storage-service" });

const batchesInserted = createCounter({ name: "raw_storage_batches_inserted_total", help: "Batches inserted to ClickHouse" });
const eventsStored = createCounter({ name: "raw_storage_events_stored_total", help: "Events stored in ClickHouse" });
const eventsDropped = createCounter({ name: "raw_storage_events_dropped_total", help: "Events dropped due to backpressure" });
const circuitState = createGauge({ name: "raw_storage_clickhouse_circuit_state", help: "ClickHouse circuit state (0=closed,1=half-open,2=open)" });
const bufferSize = createGauge({ name: "raw_storage_buffer_size", help: "Current in-memory buffer size" });
const insertDuration = createHistogram({ name: "raw_storage_insert_duration_ms", help: "ClickHouse insert duration ms", buckets: [50, 100, 250, 500, 1000, 2500, 5000] });

const CONSUMER_GROUP = "raw-storage-service";
const INPUT_TOPIC = TOPICS.ENRICHED_EVENTS;
const BATCH_SIZE = 1000;
const FLUSH_INTERVAL_MS = 5000;
const MAX_BUFFER_SIZE = 50_000;
const CIRCUIT_FAILURE_THRESHOLD = 3;
const CIRCUIT_RESET_MS = 15_000;

const kafka = getKafka({ clientId: "raw-storage-service" });

const clickhouse = new ClickHouse({
  url: process.env.CLICKHOUSE_HOST || "localhost",
  port: parseInt(process.env.CLICKHOUSE_PORT || "8123"),
  database: process.env.CLICKHOUSE_DB || "catalyst",
  username: process.env.CLICKHOUSE_USER || "catalyst",
  password: process.env.CLICKHOUSE_PASSWORD || "catalyst",
});

let consumer: Consumer;
let buffer: EnrichedEvent[] = [];
let lastFlush = Date.now();
let flushTimer: ReturnType<typeof setTimeout> | null = null;
let metricsServer: ReturnType<typeof Bun.serve> | null = null;
let inFlight = 0;
let draining = false;

async function insertBatch(rows: EnrichedEvent[]): Promise<void> {
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
    new Date(event.timestamp).toISOString(),
  ]);

  const query = `
    INSERT INTO events (project_id, event, user_id, session_id, country, city, device_type, browser, os, timestamp)
    FORMAT values
  `;

  const end = insertDuration.startTimer();
  await clickhouse.insert(query, values).toPromise();
  end();
}

const insertBreaker: TrackedBreaker<[EnrichedEvent[]], void> = createBreaker(insertBatch, {
  name: "clickhouse-insert",
  timeout: 10_000,
  errorThresholdPercentage: 50,
  resetTimeout: CIRCUIT_RESET_MS,
  volumeThreshold: CIRCUIT_FAILURE_THRESHOLD,
  fallback: () => {
    logger.warn({ bufferSize: buffer.length }, "ClickHouse circuit open");
    throw new Error("ClickHouse circuit open");
  },
});

setInterval(() => {
  circuitState.set(insertBreaker.isOpen ? 2 : insertBreaker.state === "halfOpen" ? 1 : 0);
  bufferSize.set(buffer.length);
}, 1000).unref();

async function flushBuffer() {
  if (buffer.length === 0) return;

  if (insertBreaker.isOpen) {
    return;
  }

  const rows = buffer.splice(0, BATCH_SIZE);
  if (rows.length === 0) return;

  const now = Date.now();
  logger.info({ count: rows.length }, "Flushing batch to ClickHouse");

  try {
    await insertBreaker.fire(rows);
    batchesInserted.inc();
    eventsStored.inc(rows.length);
    logger.info({ count: rows.length, ms: Date.now() - now }, "Batch inserted");
    lastFlush = Date.now();
  } catch (err) {
    logger.warn({ error: err, count: rows.length }, "ClickHouse insert failed");
    const overflow = buffer.length + rows.length - MAX_BUFFER_SIZE;
    if (overflow > 0) {
      const dropped = buffer.splice(0, overflow);
      eventsDropped.inc(dropped.length);
      logger.error({ dropped: dropped.length }, "Buffer overflow, dropped oldest events");
    }
    buffer.unshift(...rows);
  }
}

function scheduleFlush() {
  if (flushTimer) clearTimeout(flushTimer);
  const elapsed = Date.now() - lastFlush;
  const delay = Math.max(0, FLUSH_INTERVAL_MS - elapsed);
  flushTimer = setTimeout(flushBuffer, delay);
}

async function sleep(ms: number) {
  await new Promise((r) => setTimeout(r, ms));
}

async function shutdown() {
  if (draining) return;
  draining = true;
  logger.info({ inFlight, bufferSize: buffer.length }, "Shutting down raw-storage service...");

  const deadline = Date.now() + 25_000;
  while (inFlight > 0 && Date.now() < deadline) {
    await sleep(100);
  }
  if (inFlight > 0) {
    logger.warn({ inFlight }, "Shutdown deadline reached with in-flight messages");
  }

  await consumer?.stop();
  await consumer?.disconnect();
  if (flushTimer) clearTimeout(flushTimer);
  await flushBuffer();
  insertBreaker.shutdown();
  metricsServer?.stop();
  await shutdownTracing();
  await flushLogs();
  process.exit(0);
}

process.on("SIGTERM", shutdown);
process.on("SIGINT", shutdown);

async function start() {
  initTracing({ serviceName: "raw-storage-service" });
  logger.info({ topic: INPUT_TOPIC, group: CONSUMER_GROUP }, "Starting raw-storage service");

  consumer = await createConsumer(CONSUMER_GROUP);

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
        const traceparent = message.headers?.traceparent?.toString();
        const span = startSpanWithTraceContext({ traceparent }, "raw-storage.buffer");

        try {
          const event = JSON.parse(message.value.toString()) as EnrichedEvent;
          span.setAttribute("event.projectId", event.projectId);
          span.setAttribute("event.type", event.event);

          if (buffer.length >= MAX_BUFFER_SIZE) {
            buffer.shift();
            eventsDropped.inc();
            logger.warn({ maxBuffer: MAX_BUFFER_SIZE }, "Buffer at max, dropping oldest event");
          }

          buffer.push(event);

          if (buffer.length >= BATCH_SIZE) {
            if (flushTimer) clearTimeout(flushTimer);
            await flushBuffer();
          } else {
            scheduleFlush();
          }

          resolveOffset(message.offset);
          await heartbeat();
        } catch (err) {
          logger.error({ error: err, offset: message.offset }, "Error buffering event");
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

  logger.info("Raw-storage service running");
}

start().catch((err) => {
  logger.error({ error: err }, "Fatal error");
  process.exit(1);
});
