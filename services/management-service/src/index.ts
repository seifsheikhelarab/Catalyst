import { Hono } from "hono";
import pg from "pg";
import bcrypt from "bcrypt";
import { SignJWT, jwtVerify } from "jose";
import { connectRedis } from "@catalyst/redis";
import type { RedisClient } from "@catalyst/redis";
import { createLogger, flushLogs, sleep } from "@catalyst/logger";
import { getKafka, getProducer, createConsumer, TOPICS, type Consumer, type Producer, DLQEnvelopeSchema } from "@catalyst/kafka";
import type { DLQEnvelope } from "@catalyst/types";
import { initTracing, startSpan, shutdownTracing } from "@catalyst/tracing";
import { createCounter, metricsHandler } from "@catalyst/metrics";
import type { ContentfulStatusCode } from "hono/utils/http-status";
import crypto from "crypto";

const { Pool } = pg;
const logger = createLogger({ name: "management-service" });

const requestsTotal = createCounter({ name: "mgmt_requests_total", help: "Total management service requests" });
const loginSuccess = createCounter({ name: "mgmt_login_success_total", help: "Successful logins" });
const loginFailed = createCounter({ name: "mgmt_login_failed_total", help: "Failed login attempts" });
const dlqEventsStored = createCounter({ name: "mgmt_dlq_events_stored_total", help: "DLQ events persisted" });
const retriesRequested = createCounter({ name: "mgmt_dlq_retries_requested_total", help: "DLQ retries requested" });
const retriesFailed = createCounter({ name: "mgmt_dlq_retries_failed_total", help: "DLQ retries that failed to republish" });

const ACCESS_TOKEN_TTL = 15 * 60;
const REFRESH_TOKEN_TTL = 7 * 24 * 60 * 60;
const SCHEMA_CACHE_TTL = 300;

let redis: RedisClient | null = null;
let pool: pg.Pool;
let consumer: Consumer;
let producer: Producer;
let server: ReturnType<typeof Bun.serve<{ Variables: Variables }>> | null = null;
let inFlight = 0;
let draining = false;

const kafka = getKafka({ clientId: "management-service" });
const JWT_SECRET = new TextEncoder().encode(process.env.JWT_SECRET || "dev-secret-change-in-prod");
const PORT = parseInt(process.env.PORT || "3001");

type Variables = { userId: string; orgId: string };

const app = new Hono<{ Variables: Variables }>();

function getClient() {
  return pool.connect();
}

async function getRedisClient() {
  if (!redis) redis = await connectRedis();
  return redis;
}

async function signToken(payload: { sub: string; orgId: string; type: "access" | "refresh" }, expiresIn: number) {
  return new SignJWT({ ...payload })
    .setProtectedHeader({ alg: "HS256" })
    .setIssuedAt()
    .setExpirationTime(`${expiresIn}s`)
    .sign(JWT_SECRET);
}

async function verifyToken(token: string) {
  try {
    const { payload } = await jwtVerify(token, JWT_SECRET);
    return payload;
  } catch {
    return null;
  }
}

// --- Auth routes ---

app.post("/auth/register", async (c) => {
  requestsTotal.inc();
  const body = await c.req.json();
  const { email, password, orgName } = body;
  if (!email || !password) return c.json({ error: "email and password required" }, 400);

  const passwordHash = await bcrypt.hash(password, 10);
  const orgId = crypto.randomUUID();
  const userId = crypto.randomUUID();
  const client = await getClient();
  try {
    await client.query("BEGIN");
    await client.query("INSERT INTO orgs (id, name) VALUES ($1, $2)", [orgId, orgName || email.split("@")[0]]);
    await client.query("INSERT INTO users (id, org_id, email, password_hash) VALUES ($1, $2, $3, $4)", [userId, orgId, email, passwordHash]);
    await client.query("COMMIT");

    const accessToken = await signToken({ sub: userId, orgId, type: "access" }, ACCESS_TOKEN_TTL);
    const refreshToken = await signToken({ sub: userId, orgId, type: "refresh" }, REFRESH_TOKEN_TTL);
    const rclient = await getRedisClient();
    await rclient.setex(`refresh:${refreshToken}`, REFRESH_TOKEN_TTL, userId);
    return c.json({ accessToken, refreshToken, user: { id: userId, email, orgId } }, 201);
  } catch (err) {
    await client.query("ROLLBACK");
    if (err && typeof err === "object" && "code" in err && (err as Record<string, unknown>).code === "23505") return c.json({ error: "email already exists" }, 409);
    return c.json({ error: "registration failed" }, 500);
  } finally {
    client.release();
  }
});

app.post("/auth/login", async (c) => {
  requestsTotal.inc();
  const body = await c.req.json();
  const { email, password } = body;
  if (!email || !password) return c.json({ error: "email and password required" }, 400);

  const client = await getClient();
  try {
    const result = await client.query("SELECT id, org_id, email, password_hash FROM users WHERE email = $1", [email]);
    if (result.rows.length === 0) { loginFailed.inc(); return c.json({ error: "invalid credentials" }, 401); }
    const user = result.rows[0];
    const match = await bcrypt.compare(password, user.password_hash);
    if (!match) { loginFailed.inc(); return c.json({ error: "invalid credentials" }, 401); }
    loginSuccess.inc();

    const accessToken = await signToken({ sub: user.id, orgId: user.org_id, type: "access" }, ACCESS_TOKEN_TTL);
    const refreshToken = await signToken({ sub: user.id, orgId: user.org_id, type: "refresh" }, REFRESH_TOKEN_TTL);
    const rclient = await getRedisClient();
    await rclient.setex(`refresh:${refreshToken}`, REFRESH_TOKEN_TTL, user.id);
    return c.json({ accessToken, refreshToken, user: { id: user.id, email: user.email, orgId: user.org_id } });
  } finally {
    client.release();
  }
});

app.post("/auth/refresh", async (c) => {
  requestsTotal.inc();
  const body = await c.req.json();
  const { refreshToken } = body;
  if (!refreshToken) return c.json({ error: "refresh token required" }, 400);

  const payload = await verifyToken(refreshToken);
  if (!payload || payload.type !== "refresh") return c.json({ error: "invalid refresh token" }, 401);

  const rclient = await getRedisClient();
  const stored = await rclient.get(`refresh:${refreshToken}`);
  if (!stored || stored !== payload.sub) return c.json({ error: "token revoked" }, 401);

  await rclient.del(`refresh:${refreshToken}`);
  const newAccessToken = await signToken({ sub: payload.sub as string, orgId: payload.orgId as string, type: "access" }, ACCESS_TOKEN_TTL);
  const newRefreshToken = await signToken({ sub: payload.sub as string, orgId: payload.orgId as string, type: "refresh" }, REFRESH_TOKEN_TTL);
  await rclient.setex(`refresh:${newRefreshToken}`, REFRESH_TOKEN_TTL, payload.sub as string);
  return c.json({ accessToken: newAccessToken, refreshToken: newRefreshToken });
});

app.get("/auth/me", async (c) => {
  requestsTotal.inc();
  const auth = c.req.header("Authorization");
  if (!auth?.startsWith("Bearer ")) return c.json({ error: "unauthorized" }, 401);
  const token = auth.slice(7);
  const payload = await verifyToken(token);
  if (!payload || payload.type !== "access") return c.json({ error: "invalid token" }, 401);

  const client = await getClient();
  try {
    const result = await client.query("SELECT id, org_id, email FROM users WHERE id = $1", [payload.sub]);
    if (result.rows.length === 0) return c.json({ error: "user not found" }, 404);
    return c.json(result.rows[0]);
  } finally {
    client.release();
  }
});

app.post("/auth/logout", async (c) => {
  requestsTotal.inc();
  const body = await c.req.json();
  const { refreshToken } = body;
  if (refreshToken) {
    const rclient = await getRedisClient();
    await rclient.del(`refresh:${refreshToken}`);
  }
  return c.json({ ok: true });
});

// --- Project routes ---

app.post("/internal/keys/:key/validate", async (c) => {
  requestsTotal.inc();
  const presentedKey = c.req.param("key");
  if (!presentedKey.startsWith("pk_live_")) return c.json({ valid: false }, 401);
  const prefix = presentedKey.slice(0, 16);
  const client = await getClient();
  try {
    const rows = await client.query(
      `SELECT ak.key_hash, ak.project_id, p.org_id FROM api_keys ak JOIN projects p ON p.id = ak.project_id WHERE ak.key_prefix = $1 AND (ak.expires_at IS NULL OR ak.expires_at > NOW())`,
      [prefix],
    );
    if (rows.rows.length === 0) return c.json({ valid: false }, 401);
    for (const row of rows.rows) {
      if (await bcrypt.compare(presentedKey, row.key_hash)) {
        return c.json({ valid: true, projectId: row.project_id, orgId: row.org_id });
      }
    }
    return c.json({ valid: false }, 401);
  } finally {
    client.release();
  }
});

app.post("/projects", async (c) => {
  requestsTotal.inc();
  const body = await c.req.json();
  const { name, orgId } = body;
  if (!name || !orgId) return c.json({ error: "name and orgId required" }, 400);
  const client = await getClient();
  try {
    const result = await client.query("INSERT INTO projects (name, org_id) VALUES ($1, $2) RETURNING *", [name, orgId]);
    return c.json(result.rows[0], 201);
  } finally {
    client.release();
  }
});

app.get("/projects/:id", async (c) => {
  requestsTotal.inc();
  const id = c.req.param("id");
  const client = await getClient();
  try {
    const result = await client.query("SELECT * FROM projects WHERE id = $1", [id]);
    if (result.rows.length === 0) return c.json({ error: "not found" }, 404);
    return c.json(result.rows[0]);
  } finally {
    client.release();
  }
});

app.get("/projects", async (c) => {
  requestsTotal.inc();
  const orgId = c.req.query("orgId");
  const client = await getClient();
  try {
    const result = orgId
      ? await client.query("SELECT * FROM projects WHERE org_id = $1 ORDER BY created_at DESC", [orgId])
      : await client.query("SELECT * FROM projects ORDER BY created_at DESC");
    return c.json(result.rows);
  } finally {
    client.release();
  }
});

app.post("/projects/:id/keys", async (c) => {
  requestsTotal.inc();
  const projectId = c.req.param("id");
  const body = await c.req.json().catch(() => ({}));
  const name = body.name || "default";
  const rawKey = `pk_live_${crypto.randomBytes(32).toString("hex")}`;
  const hash = await bcrypt.hash(rawKey, 10);
  const prefix = rawKey.slice(0, 16);
  const client = await getClient();
  try {
    const result = await client.query(
      "INSERT INTO api_keys (project_id, key_hash, key_prefix, name) VALUES ($1, $2, $3, $4) RETURNING id, created_at",
      [projectId, hash, prefix, name],
    );
    return c.json({ key: rawKey, id: result.rows[0].id, name, createdAt: result.rows[0].created_at }, 201);
  } finally {
    client.release();
  }
});

app.get("/projects/:id/keys", async (c) => {
  requestsTotal.inc();
  const projectId = c.req.param("id");
  const client = await getClient();
  try {
    const result = await client.query("SELECT id, name, created_at, expires_at FROM api_keys WHERE project_id = $1", [projectId]);
    return c.json(result.rows);
  } finally {
    client.release();
  }
});

app.post("/projects/:id/schemas", async (c) => {
  requestsTotal.inc();
  const projectId = c.req.param("id");
  const body = await c.req.json();
  const { eventName, schema } = body;
  if (!eventName || !schema) return c.json({ error: "eventName and schema required" }, 400);
  const client = await getClient();
  try {
    const result = await client.query(
      `INSERT INTO event_schemas (project_id, event_name, schema) VALUES ($1, $2, $3) ON CONFLICT (project_id, event_name) DO UPDATE SET schema = EXCLUDED.schema, version = event_schemas.version + 1, updated_at = NOW() RETURNING *`,
      [projectId, eventName, JSON.stringify(schema)],
    );
    const rclient = await getRedisClient();
    await rclient.del(`schema:${projectId}:${eventName}`);
    return c.json(result.rows[0], 201);
  } finally {
    client.release();
  }
});

app.get("/projects/:id/schemas", async (c) => {
  requestsTotal.inc();
  const projectId = c.req.param("id");
  const client = await getClient();
  try {
    const result = await client.query("SELECT * FROM event_schemas WHERE project_id = $1", [projectId]);
    return c.json(result.rows);
  } finally {
    client.release();
  }
});

app.get("/projects/:id/schemas/:eventName", async (c) => {
  requestsTotal.inc();
  const projectId = c.req.param("id");
  const eventName = c.req.param("eventName");
  const rclient = await getRedisClient();
  const cacheKey = `schema:${projectId}:${eventName}`;
  const cached = await rclient.get(cacheKey);
  if (cached) return c.json(JSON.parse(cached));
  const client = await getClient();
  try {
    const result = await client.query("SELECT * FROM event_schemas WHERE project_id = $1 AND event_name = $2", [projectId, eventName]);
    if (result.rows.length === 0) return c.json({ error: "not found" }, 404);
    const schema = result.rows[0];
    await rclient.setex(cacheKey, SCHEMA_CACHE_TTL, JSON.stringify(schema));
    return c.json(schema);
  } finally {
    client.release();
  }
});

// --- DLQ Admin routes ---

app.get("/admin/dlq", async (c) => {
  const status = c.req.query("status") || "pending";
  const limit = Math.max(1, Math.min(500, parseInt(c.req.query("limit") || "50", 10)));
  const offset = Math.max(0, parseInt(c.req.query("offset") || "0", 10));
  const result = await pool.query(
    `SELECT id, original_topic, original_partition, original_offset, original_key, reason, status, retry_count, created_at, updated_at FROM dlq_events WHERE status = $1 ORDER BY created_at DESC LIMIT $2 OFFSET $3`,
    [status, limit, offset],
  );
  const count = await pool.query("SELECT count(*)::int AS total FROM dlq_events WHERE status = $1", [status]);
  return c.json({ total: count.rows[0].total, events: result.rows });
});

app.get("/admin/dlq/:id", async (c) => {
  const id = parseInt(c.req.param("id"), 10);
  if (isNaN(id)) return c.json({ error: "invalid id" }, 400);
  const result = await pool.query("SELECT * FROM dlq_events WHERE id = $1", [id]);
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
    const status = (apiErr?.statusCode ?? 500) as ContentfulStatusCode;
    return c.json({ error: apiErr?.message ?? "retry failed" }, status);
  }
});

app.post("/admin/dlq/:id/discard", async (c) => {
  const id = parseInt(c.req.param("id"), 10);
  if (isNaN(id)) return c.json({ error: "invalid id" }, 400);
  const result = await pool.query("UPDATE dlq_events SET status = 'discarded', updated_at = NOW() WHERE id = $1 AND status = 'pending' RETURNING id", [id]);
  if (result.rowCount === 0) return c.json({ error: "not found or not pending" }, 404);
  return c.json({ ok: true, id });
});

async function storeEnvelope(envelope: DLQEnvelope): Promise<number> {
  const result = await pool.query(
    `INSERT INTO dlq_events (original_topic, original_partition, original_offset, original_key, original_value, original_headers, reason) VALUES ($1, $2, $3, $4, $5, $6, $7) RETURNING id`,
    [envelope.originalTopic, envelope.originalPartition, envelope.originalOffset, envelope.originalKey ?? null, envelope.originalValue ?? null, envelope.originalHeaders ?? null, envelope.reason],
  );
  return result.rows[0].id as number;
}

async function retryEvent(id: number): Promise<{ topic: string; status: string }> {
  const result = await pool.query("SELECT * FROM dlq_events WHERE id = $1 AND status = 'pending' FOR UPDATE", [id]);
  if (result.rowCount === 0) {
    const error = new Error(`DLQ event ${id} not found or not in pending state`) as Error & { statusCode: number };
    error.statusCode = 404;
    throw error;
  }
  const row = result.rows[0];
  await pool.query("UPDATE dlq_events SET status = 'retrying', updated_at = NOW() WHERE id = $1", [id]);
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
      messages: [{ key: row.original_key, value: row.original_value, headers: Object.fromEntries(Object.entries(headers).map(([k, v]) => [k, Buffer.from(v as string)])) }],
    });
    await pool.query("UPDATE dlq_events SET status = 'retried', retry_count = retry_count + 1, updated_at = NOW() WHERE id = $1", [id]);
    retriesRequested.inc();
    logger.info({ id, topic: row.original_topic }, "DLQ event retried");
    return { topic: row.original_topic, status: "retried" };
  } catch (err) {
    await pool.query("UPDATE dlq_events SET status = 'pending', last_error = $2, updated_at = NOW() WHERE id = $1", [id, err instanceof Error ? err.message : String(err)]);
    retriesFailed.inc();
    throw err;
  } finally {
    span.end();
  }
}

app.get("/metrics", async (_c) => metricsHandler());
app.get("/health", (c) => c.json({ status: "ok" }));

async function shutdown() {
  if (draining) return;
  draining = true;
  logger.info({ inFlight }, "Shutting down management service...");
  const deadline = Date.now() + 25_000;
  while (inFlight > 0 && Date.now() < deadline) await sleep(100);
  if (inFlight > 0) logger.warn({ inFlight }, "Shutdown deadline reached with in-flight messages");
  server?.stop();
  await consumer?.stop();
  await consumer?.disconnect();
  await producer?.disconnect();
  await pool?.end();
  await redis?.quit();
  await shutdownTracing();
  await flushLogs();
  process.exit(0);
}

process.on("SIGTERM", shutdown);
process.on("SIGINT", shutdown);

async function start() {
  initTracing({ serviceName: "management-service" });

  pool = new Pool({
    host: process.env.POSTGRES_HOST || "localhost",
    port: parseInt(process.env.POSTGRES_PORT || "5432"),
    user: process.env.POSTGRES_USER || "catalyst",
    password: process.env.POSTGRES_PASSWORD || "catalyst",
    database: process.env.POSTGRES_DB || "catalyst",
    max: 20,
  });

  await getRedisClient();

  // Start DLQ consumer
  consumer = await createConsumer("management-dlq");
  producer = await getProducer();
  await consumer.subscribe({ topic: TOPICS.DEAD_LETTER, fromBeginning: false });
  await consumer.run({
    autoCommit: false,
    eachBatch: async ({ batch, resolveOffset, commitOffsetsIfNecessary, heartbeat, isRunning, isStale }) => {
      for (const message of batch.messages) {
        if (!isRunning() || isStale()) break;
        if (!message.value) { resolveOffset(message.offset); continue; }
        inFlight++;
        try {
          const span = startSpan("dlq.persist", { "kafka.offset": message.offset });
          try {
            const raw = JSON.parse(message.value.toString());
            const envelope = DLQEnvelopeSchema.parse(raw);
            const id = await storeEnvelope(envelope);
            dlqEventsStored.inc();
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

  server = Bun.serve({ port: PORT, fetch: app.fetch });
  logger.info({ port: PORT }, "Management service running");
}

start().catch((err) => {
  logger.error({ error: err }, "Fatal error");
  process.exit(1);
});
