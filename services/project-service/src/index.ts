import { Hono } from "hono";
import pg from "pg";
import bcrypt from "bcrypt";
import { connectRedis } from "@catalyst/redis";
import { createLogger } from "@catalyst/logger";
import crypto from "crypto";
import { initTracing, startSpan } from "@catalyst/tracing";
import { createCounter, metricsHandler } from "@catalyst/metrics";

const { Pool } = pg;
const logger = createLogger({ name: "project-service" });
const requestsTotal = createCounter({ name: "project_requests_total", help: "Total project service requests" });

const app = new Hono();
let redis: any;
let pool: pg.Pool;
const SCHEMA_CACHE_TTL = 300;

function getClient() {
  return pool.connect();
}

async function getRedisClient() {
  if (!redis) redis = await connectRedis();
  return redis;
}

async function shutdown() {
  logger.info("Shutting down...");
  await pool?.end();
  await redis?.quit();
  process.exit(0);
}

process.on("SIGTERM", shutdown);
process.on("SIGINT", shutdown);

app.post("/internal/keys/:key/validate", async (c) => {
  requestsTotal.inc();
  const presentedKey = c.req.param("key");
  if (!presentedKey.startsWith("pk_live_")) return c.json({ valid: false }, 401);
  const prefix = presentedKey.slice(0, 16);
  const client = await getClient();
  try {
    const rows = await client.query(
      `SELECT ak.key_hash, ak.project_id, p.org_id
       FROM api_keys ak
       JOIN projects p ON p.id = ak.project_id
       WHERE ak.key_prefix = $1 AND (ak.expires_at IS NULL OR ak.expires_at > NOW())`,
      [prefix],
    );
    if (rows.rows.length === 0) return c.json({ valid: false }, 401);
    for (const row of rows.rows) {
      const match = await bcrypt.compare(presentedKey, row.key_hash);
      if (match) {
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
    const result = await client.query(
      "INSERT INTO projects (name, org_id) VALUES ($1, $2) RETURNING *",
      [name, orgId],
    );
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
    let result;
    if (orgId) {
      result = await client.query(
        "SELECT * FROM projects WHERE org_id = $1 ORDER BY created_at DESC",
        [orgId],
      );
    } else {
      result = await client.query("SELECT * FROM projects ORDER BY created_at DESC");
    }
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
    return c.json(
      { key: rawKey, id: result.rows[0].id, name, createdAt: result.rows[0].created_at },
      201,
    );
  } finally {
    client.release();
  }
});

app.get("/projects/:id/keys", async (c) => {
  requestsTotal.inc();
  const projectId = c.req.param("id");
  const client = await getClient();
  try {
    const result = await client.query(
      "SELECT id, name, created_at, expires_at FROM api_keys WHERE project_id = $1",
      [projectId],
    );
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
      `INSERT INTO event_schemas (project_id, event_name, schema)
       VALUES ($1, $2, $3)
       ON CONFLICT (project_id, event_name) DO UPDATE SET schema = EXCLUDED.schema, version = event_schemas.version + 1, updated_at = NOW()
       RETURNING *`,
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
    const result = await client.query("SELECT * FROM event_schemas WHERE project_id = $1", [
      projectId,
    ]);
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
    const result = await client.query(
      "SELECT * FROM event_schemas WHERE project_id = $1 AND event_name = $2",
      [projectId, eventName],
    );
    if (result.rows.length === 0) return c.json({ error: "not found" }, 404);
    const schema = result.rows[0];
    await rclient.setex(cacheKey, SCHEMA_CACHE_TTL, JSON.stringify(schema));
    return c.json(schema);
  } finally {
    client.release();
  }
});

app.get("/metrics", (_c) => metricsHandler());
app.get("/health", (c) => {
  requestsTotal.inc();
  return c.json({ status: "ok" });
});

const port = parseInt(process.env.PORT || "3001");
export default { port, fetch: app.fetch };

async function start() {
  initTracing({ serviceName: "project-service" });
  pool = new Pool({
    host: process.env.POSTGRES_HOST || "localhost",
    port: parseInt(process.env.POSTGRES_PORT || "5432"),
    user: process.env.POSTGRES_USER || "catalyst",
    password: process.env.POSTGRES_PASSWORD || "catalyst",
    database: process.env.POSTGRES_DB || "catalyst",
    max: 20,
  });

  await getRedisClient();
  logger.info({ port }, "Project service running");
}

start().catch((err) => {
  logger.error({ error: err }, "Fatal error");
  process.exit(1);
});
