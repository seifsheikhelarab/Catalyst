import { Hono } from "hono";
import pg from "pg";
import { getKafka, getProducer, createConsumer, TOPICS, type Consumer, type Producer, DLQEnvelopeSchema } from "@catalyst/kafka";
import type { DLQEnvelope } from "@catalyst/types";
import { createLogger, flushLogs } from "@catalyst/logger";
import { initTracing, startSpan, shutdownTracing } from "@catalyst/tracing";
import { createCounter, metricsHandler } from "@catalyst/metrics";
import type { ContentfulStatusCode } from "hono/utils/http-status";

const { Pool } = pg;
const logger = createLogger({ name: "dlq-processor-service" });

const eventsStored = createCounter({ name: "dlq_events_stored_total", help: "DLQ events persisted" });
const retriesRequested = createCounter({ name: "dlq_retries_requested_total", help: "DLQ retries requested" });
const retriesFailed = createCounter({ name: "dlq_retries_failed_total", help: "DLQ retries that failed to republish" });

const CONSUMER_GROUP = "dlq-processor-service";
const INPUT_TOPIC = TOPICS.DEAD_LETTER;
const PORT = parseInt(process.env.PORT || "3006");

const kafka = getKafka({ clientId: "dlq-processor-service" });

let consumer: Consumer;
let producer: Producer;
let pgPool: pg.Pool;
let server: ReturnType<typeof Bun.serve> | null = null;
let inFlight = 0;
let draining = false;

async function shutdown() {
  if (draining) return;
  draining = true;
  logger.info({ inFlight }, "Shutting down DLQ processor...");

  const deadline = Date.now() + 25_000;
  while (inFlight > 0 && Date.now() < deadline) {
    await new Promise((r) => setTimeout(r, 100));
  }
  if (inFlight > 0) {
    logger.warn({ inFlight }, "Shutdown deadline reached with in-flight messages");
  }

  server?.stop();
  await consumer?.stop();
  await consumer?.disconnect();
  await producer?.disconnect();
  await pgPool?.end();
  await shutdownTracing();
  await flushLogs();
  process.exit(0);
}

process.on("SIGTERM", shutdown);
process.on("SIGINT", shutdown);

async function storeEnvelope(envelope: DLQEnvelope): Promise<number> {
  const result = await pgPool.query(
    `INSERT INTO dlq_events
       (original_topic, original_partition, original_offset, original_key, original_value, original_headers, reason)
     VALUES ($1, $2, $3, $4, $5, $6, $7)
     RETURNING id`,
    [
      envelope.originalTopic,
      envelope.originalPartition,
      envelope.originalOffset,
      envelope.originalKey ?? null,
      envelope.originalValue ?? null,
      envelope.originalHeaders ?? null,
      envelope.reason,
    ],
  );
  return result.rows[0].id as number;
}

async function retryEvent(id: number): Promise<{ topic: string; status: string }> {
  const result = await pgPool.query(
    `SELECT * FROM dlq_events WHERE id = $1 AND status = 'pending' FOR UPDATE`,
    [id],
  );
  if (result.rowCount === 0) {
    const error = new Error(`DLQ event ${id} not found or not in pending state`) as Error & { statusCode: number };
    error.statusCode = 404;
    throw error;
  }
  const row = result.rows[0];

  // Mark as retrying before attempting
  await pgPool.query(
    `UPDATE dlq_events SET status = 'retrying', updated_at = NOW() WHERE id = $1`,
    [id],
  );

  const span = startSpan("dlq.retry", { "dlq.id": id, "dlq.topic": row.original_topic });
  try {
    const headers: Record<string, string | Buffer> = {};
    if (row.original_headers && typeof row.original_headers === "object") {
      for (const [k, v] of Object.entries(row.original_headers)) {
        headers[k] = typeof v === "string" ? v : JSON.stringify(v);
      }
    }

    await producer.send({
      topic: row.original_topic,
      messages: [
        {
          key: row.original_key,
          value: row.original_value,
          headers: Object.fromEntries(
            Object.entries(headers).map(([k, v]) => [k, Buffer.from(v as string)]),
          ),
        },
      ],
    });

    await pgPool.query(
      `UPDATE dlq_events SET status = 'retried', retry_count = retry_count + 1, updated_at = NOW() WHERE id = $1`,
      [id],
    );
    retriesRequested.inc();
    logger.info({ id, topic: row.original_topic }, "DLQ event retried");
    return { topic: row.original_topic, status: "retried" };
  } catch (err) {
    await pgPool.query(
      `UPDATE dlq_events SET status = 'pending', last_error = $2, updated_at = NOW() WHERE id = $1`,
      [id, err instanceof Error ? err.message : String(err)],
    );
    retriesFailed.inc();
    throw err;
  } finally {
    span.end();
  }
}

const app = new Hono();

app.get("/metrics", async (_c) => metricsHandler());
app.get("/health", (c) => c.json({ status: "ok" }));

app.get("/admin/dlq", async (c) => {
  const status = c.req.query("status") || "pending";
  const limit = Math.max(1, Math.min(500, parseInt(c.req.query("limit") || "50", 10)));
  const offset = Math.max(0, parseInt(c.req.query("offset") || "0", 10));

  const result = await pgPool.query(
    `SELECT id, original_topic, original_partition, original_offset, original_key,
            reason, status, retry_count, created_at, updated_at
     FROM dlq_events
     WHERE status = $1
     ORDER BY created_at DESC
     LIMIT $2 OFFSET $3`,
    [status, limit, offset],
  );
  const count = await pgPool.query(
    `SELECT count(*)::int AS total FROM dlq_events WHERE status = $1`,
    [status],
  );
  return c.json({ total: count.rows[0].total, events: result.rows });
});

app.get("/admin/dlq/:id", async (c) => {
  const id = parseInt(c.req.param("id"), 10);
  if (isNaN(id)) return c.json({ error: "invalid id" }, 400);
  const result = await pgPool.query(`SELECT * FROM dlq_events WHERE id = $1`, [id]);
  if (result.rowCount === 0) return c.json({ error: "not found" }, 404);
  return c.json(result.rows[0]);
});

app.post("/admin/dlq/:id/retry", async (c) => {
  const id = parseInt(c.req.param("id"), 10);
  if (isNaN(id)) return c.json({ error: "invalid id" }, 400);
  try {
    const result = await retryEvent(id);
    return c.json({ ok: true, ...result });
  } catch (err) {
    const apiErr = err as Error & { statusCode?: number };
    const status = apiErr?.statusCode ?? 500;
    return c.json({ error: apiErr?.message ?? "retry failed" }, status);
  }
});

app.post("/admin/dlq/:id/discard", async (c) => {
  const id = parseInt(c.req.param("id"), 10);
  if (isNaN(id)) return c.json({ error: "invalid id" }, 400);
  const result = await pgPool.query(
    `UPDATE dlq_events SET status = 'discarded', updated_at = NOW() WHERE id = $1 AND status = 'pending' RETURNING id`,
    [id],
  );
  if (result.rowCount === 0) return c.json({ error: "not found or not pending" }, 404);
  return c.json({ ok: true, id });
});

async function start() {
  initTracing({ serviceName: "dlq-processor-service" });
  logger.info({ topic: INPUT_TOPIC, group: CONSUMER_GROUP }, "Starting DLQ processor");

  pgPool = new Pool({
    host: process.env.POSTGRES_HOST || "localhost",
    port: parseInt(process.env.POSTGRES_PORT || "5432"),
    user: process.env.POSTGRES_USER || "catalyst",
    password: process.env.POSTGRES_PASSWORD || "catalyst",
    database: process.env.POSTGRES_DB || "catalyst",
    max: 10,
  });

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
        try {
          const span = startSpan("dlq.persist", { "kafka.offset": message.offset });
          try {
            const raw = JSON.parse(message.value.toString());
            const envelope = DLQEnvelopeSchema.parse(raw);
            const id = await storeEnvelope(envelope);
            eventsStored.inc();
            logger.info({ id, originalTopic: envelope.originalTopic, reason: envelope.reason.slice(0, 200) }, "DLQ event stored");
          } catch (err) {
            logger.error({ error: err, offset: message.offset }, "Failed to persist DLQ envelope");
          } finally {
            span.end();
          }
          resolveOffset(message.offset);
          await heartbeat();
        } finally {
          inFlight--;
        }
      }
      await commitOffsetsIfNecessary();
    },
  });

  server = Bun.serve({
    port: PORT,
    fetch: app.fetch,
  });

  logger.info({ port: PORT }, "DLQ processor running");
}

start().catch((err) => {
  logger.error({ error: err }, "Fatal error");
  process.exit(1);
});
