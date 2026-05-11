import { getKafka, TOPICS } from "@catalyst/kafka";
import { type EnrichedEvent } from "@catalyst/types";
import { createLogger } from "@catalyst/logger";
import { ClickHouse } from "clickhouse";

const logger = createLogger({ name: "raw-storage-service" });

const CONSUMER_GROUP = "raw-storage-service";
const INPUT_TOPIC = TOPICS.ENRICHED_EVENTS;

const BATCH_SIZE = 1000;
const FLUSH_INTERVAL_MS = 5000;

const kafka = getKafka({ clientId: "raw-storage-service" });

const clickhouse = new ClickHouse({
  url: process.env.CLICKHOUSE_HOST || "localhost",
  port: parseInt(process.env.CLICKHOUSE_PORT || "8123"),
  database: process.env.CLICKHOUSE_DB || "catalyst",
  username: process.env.CLICKHOUSE_USER || "catalyst",
  password: process.env.CLICKHOUSE_PASSWORD || "catalyst",
});

let consumer: any;
let buffer: EnrichedEvent[] = [];
let lastFlush = Date.now();
let flushTimer: any = null;

async function flushBuffer() {
  if (buffer.length === 0) return;

  const rows = buffer.splice(0);
  const now = Date.now();
  logger.info({ count: rows.length }, "Flushing batch to ClickHouse");

  const MAX_RETRIES = 3;
  for (let attempt = 1; attempt <= MAX_RETRIES; attempt++) {
    try {
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

      await clickhouse.insert(query).values(values).toPromise();
      logger.info({ count: rows.length, ms: Date.now() - now }, "Batch inserted");
      lastFlush = Date.now();
      return;
    } catch (err) {
      logger.warn({ error: err, count: rows.length, attempt }, "Failed to insert batch");
      if (attempt === MAX_RETRIES) {
        logger.error({ error: err, count: rows.length }, "Dropping batch after max retries");
        return;
      }
      await Bun.sleep(attempt * 1000);
    }
  }
}

function scheduleFlush() {
  if (flushTimer) clearTimeout(flushTimer);
  const elapsed = Date.now() - lastFlush;
  const delay = Math.max(0, FLUSH_INTERVAL_MS - elapsed);
  flushTimer = setTimeout(flushBuffer, delay);
}

async function shutdown() {
  logger.info("Shutting down...");
  await consumer?.stop();
  await consumer?.disconnect();
  await flushBuffer();
  if (flushTimer) clearTimeout(flushTimer);
  process.exit(0);
}

process.on("SIGTERM", shutdown);
process.on("SIGINT", shutdown);

async function start() {
  logger.info({ topic: INPUT_TOPIC, group: CONSUMER_GROUP }, "Starting raw-storage service");

  consumer = kafka.consumer({ groupId: CONSUMER_GROUP });
  await consumer.connect();

  await consumer.subscribe({ topic: INPUT_TOPIC, fromBeginning: false });

  await consumer.run({
    eachMessage: async ({ message }) => {
      const rawValue = message.value?.toString();
      if (!rawValue) return;

      try {
        const event = JSON.parse(rawValue) as EnrichedEvent;
        buffer.push(event);

        if (buffer.length >= BATCH_SIZE) {
          if (flushTimer) clearTimeout(flushTimer);
          await flushBuffer();
        } else {
          scheduleFlush();
        }
      } catch (err) {
        logger.error({ error: err }, "Error buffering event");
      }
    },
  });

  logger.info("Raw-storage service running");
}

start().catch((err) => {
  logger.error({ error: err }, "Fatal error");
  process.exit(1);
});
