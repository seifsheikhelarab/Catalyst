import { Hono } from "hono";
import pg from "pg";
import { connectRedis } from "@catalyst/redis";
import { createLogger, flushLogs } from "@catalyst/logger";
import crypto from "crypto";
import { initTracing, startSpan, shutdownTracing } from "@catalyst/tracing";
import { createCounter, createHistogram, metricsHandler } from "@catalyst/metrics";
import type { RedisClient } from "@catalyst/redis";

const { Pool } = pg;
const logger = createLogger({ name: "query-api-service" });

const queryTotal = createCounter({ name: "query_api_requests_total", help: "Total query API requests" });
const queryDuration = createHistogram({ name: "query_api_duration_ms", help: "Query duration ms", buckets: [10, 25, 50, 100, 250, 500, 1000, 2500] });
const cacheHits = createCounter({ name: "query_api_cache_hits_total", help: "Cache hits" });

type Variables = { orgId: string };

const app = new Hono<{ Variables: Variables }>();
let pgPool: pg.Pool;
let redis: RedisClient | null = null;

const CACHE_TTL = 30;

async function getRedisClient() {
  if (!redis) redis = await connectRedis();
  return redis;
}

async function shutdown() {
  logger.info("Shutting down...");
  await pgPool?.end();
  await redis?.quit();
  await shutdownTracing();
  await flushLogs();
  process.exit(0);
}

process.on("SIGTERM", shutdown);
process.on("SIGINT", shutdown);

const MANAGEMENT_SERVICE = process.env.MANAGEMENT_SERVICE_URL || "http://localhost:3001";

app.get("/metrics", async (_c) => metricsHandler());

app.use("/projects/:id/*", async (c, next) => {
  const orgId = c.req.header("X-Org-Id");
  if (!orgId) return c.json({ error: "forbidden" }, 403);

  const projectId = c.req.param("id");
  if (!projectId) return c.json({ error: "projectId required" }, 400);

  const rclient = await getRedisClient();
  const cacheKey = `project-org:${projectId}`;
  const cached = await rclient.get(cacheKey);
  if (cached && cached !== orgId) return c.json({ error: "forbidden" }, 403);
  if (!cached) {
    try {
      const res = await fetch(`${MANAGEMENT_SERVICE}/projects/${projectId}`);
      if (!res.ok) return c.json({ error: "not found" }, 404);
      const project = await res.json() as Record<string, unknown>;
      if (project.org_id !== orgId) return c.json({ error: "forbidden" }, 403);
      await rclient.setex(cacheKey, 300, String(project.org_id));
    } catch {
      return c.json({ error: "upstream unavailable" }, 502);
    }
  }

  c.set("orgId", orgId);
  await next();
});

app.get("/projects/:id/metrics", async (c) => {
  const projectId = c.req.param("id");
  const event = c.req.query("event");
  const from = c.req.query("from");
  const to = c.req.query("to");
  const granularity = c.req.query("granularity") || "hour";

  if (!projectId) return c.json({ error: "projectId required" }, 400);
  if (!event || !from || !to) return c.json({ error: "event, from, and to required" }, 400);

  const cacheKey = `query:${crypto.createHash("sha1").update(["metrics", projectId, event, from, to, granularity].join(":")).digest("hex")}`;
  const rclient = await getRedisClient();
  const cached = await rclient.get(cacheKey);
  if (cached) { cacheHits.inc(); return c.json(JSON.parse(cached)); }

  let trunc: string;
  switch (granularity) {
    case "minute": trunc = "minute"; break;
    case "day": trunc = "day"; break;
    default: trunc = "hour"; break;
  }

  try {
    const result = await pgPool.query(
      `SELECT date_trunc($1, bucket) AS bucket, SUM(count)::bigint AS count, SUM(unique_users)::bigint AS unique_users FROM event_rollups WHERE project_id = $2 AND event = $3 AND bucket BETWEEN $4 AND $5 GROUP BY 1 ORDER BY 1`,
      [trunc, projectId, event, from, to],
    );
    const data = result.rows;
    await rclient.setex(cacheKey, CACHE_TTL, JSON.stringify(data));
    queryTotal.inc();
    return c.json(data);
  } catch (err) {
    logger.error({ error: err }, "Metrics query failed");
    return c.json({ error: "query failed" }, 500);
  }
});

app.get("/projects/:id/funnels", async (c) => {
  const projectId = c.req.param("id");
  const stepsParam = c.req.query("steps");
  const windowParam = c.req.query("window") || "7d";
  const from = c.req.query("from");
  const to = c.req.query("to");

  if (!projectId) return c.json({ error: "projectId required" }, 400);
  if (!stepsParam) return c.json({ error: "steps required" }, 400);

  let steps: string[];
  try { steps = JSON.parse(stepsParam); } catch { return c.json({ error: "steps must be a JSON array" }, 400); }
  if (steps.length < 2) return c.json({ error: "at least 2 steps required" }, 400);

  const windowSeconds = parseWindow(windowParam);

  try {
    const result: Array<{ from: string; to: string; users: number }> = [];
    for (let i = 0; i < steps.length - 1; i++) {
      const q = `
        SELECT COUNT(DISTINCT a.user_id) AS users
        FROM events a
        JOIN events b ON a.user_id = b.user_id AND a.project_id = b.project_id
        WHERE a.project_id = $1 AND a.event = $2 AND b.event = $3
          AND b."timestamp" >= a."timestamp"
          AND b."timestamp" <= a."timestamp" + INTERVAL '${windowSeconds} seconds'
          AND a."timestamp" >= $4 AND a."timestamp" <= $5
      `;
      const res = await pgPool.query(q, [projectId, steps[i], steps[i + 1], from || "2000-01-01", to || "2099-12-31"]);
      result.push({ from: steps[i], to: steps[i + 1], users: parseInt(res.rows[0]?.users || "0", 10) });
    }
    queryTotal.inc();
    return c.json(result);
  } catch (err) {
    logger.error({ error: err }, "Funnel query failed");
    return c.json({ error: "query failed" }, 500);
  }
});

app.get("/projects/:id/retention", async (c) => {
  const projectId = c.req.param("id");
  const cohortEvent = c.req.query("cohortEvent");
  const returnEvent = c.req.query("returnEvent");
  const periods = parseInt(c.req.query("periods") || "8", 10);
  const from = c.req.query("from");
  const to = c.req.query("to");

  if (!projectId) return c.json({ error: "projectId required" }, 400);
  if (!cohortEvent || !returnEvent) return c.json({ error: "cohortEvent and returnEvent required" }, 400);

  try {
    const q = `
      WITH cohorts AS (
        SELECT user_id, DATE_TRUNC('week', "timestamp") AS cohort_week
        FROM events
        WHERE project_id = $1 AND event = $2 AND "timestamp" >= $3 AND "timestamp" <= $4
        GROUP BY user_id, DATE_TRUNC('week', "timestamp")
      ),
      returns AS (
        SELECT DISTINCT c.user_id, c.cohort_week, DATE_TRUNC('week', e."timestamp") AS return_week
        FROM cohorts c
        JOIN events e ON c.user_id = e.user_id
        WHERE e.project_id = $1 AND e.event = $5
      )
      SELECT cohort_week, return_week, COUNT(DISTINCT user_id) AS users
      FROM returns
      WHERE return_week >= cohort_week
      GROUP BY cohort_week, return_week
      ORDER BY cohort_week, return_week
    `;
    const res = await pgPool.query(q, [projectId, cohortEvent, from || "2000-01-01", to || "2099-12-31", returnEvent]);
    const rows = res.rows;

    const cohorts: Map<string, Map<number, number>> = new Map();
    for (const row of rows) {
      const cw = String(row.cohort_week).slice(0, 10);
      const rw = String(row.return_week).slice(0, 10);
      const weekDiff = Math.round((new Date(rw).getTime() - new Date(cw).getTime()) / (7 * 86400000));
      if (!cohorts.has(cw)) cohorts.set(cw, new Map());
      cohorts.get(cw)!.set(weekDiff, parseInt(row.users, 10));
    }
    const result: Array<Record<string, unknown>> = [];
    for (const [cohort, weeks] of cohorts) {
      const row: Record<string, unknown> = { cohort, total: 0 };
      for (let p = 0; p < periods; p++) {
        row[`period_${p}`] = weeks.get(p) || 0;
        if (p === 0) row.total = weeks.get(p) || 0;
      }
      result.push(row);
    }
    queryTotal.inc();
    return c.json(result);
  } catch (err) {
    logger.error({ error: err }, "Retention query failed");
    return c.json({ error: "query failed" }, 500);
  }
});

app.get("/projects/:id/users", async (c) => {
  const projectId = c.req.param("id");
  const filter = c.req.query("filter") || "";
  const limit = Math.max(1, Math.min(1000, parseInt(c.req.query("limit") || "50", 10)));
  const offset = Math.max(0, parseInt(c.req.query("offset") || "0", 10));

  if (!projectId) return c.json({ error: "projectId required" }, 400);

  try {
    const params: Array<string | number> = [projectId];
    let whereClause = `e.project_id = $1 AND e.user_id != ''`;
    if (filter) {
      const colonIdx = filter.indexOf(":");
      const field = colonIdx >= 0 ? filter.slice(0, colonIdx) : "";
      const value = colonIdx >= 0 ? filter.slice(colonIdx + 1) : "";
      if (field && value) {
        params.push(value);
        whereClause += ` AND e.${field.replace(/[^a-zA-Z_]/g, "")} = $${params.length}`;
      }
    }

    const q = `
      SELECT e.user_id, e.country, e.city, e.browser, e.os, e.device_type, MIN(e."timestamp") AS first_seen, MAX(e."timestamp") AS last_seen
      FROM events e
      WHERE ${whereClause}
      GROUP BY e.user_id, e.country, e.city, e.browser, e.os, e.device_type
      ORDER BY last_seen DESC
      LIMIT $${params.length + 1} OFFSET $${params.length + 2}
    `;
    params.push(limit, offset);
    const res = await pgPool.query(q, params);
    queryTotal.inc();
    return c.json(res.rows);
  } catch (err) {
    logger.error({ error: err }, "Users query failed");
    return c.json({ error: "query failed" }, 500);
  }
});

app.get("/projects/:id/events/live", async (c) => {
  const projectId = c.req.param("id");
  if (!projectId) return c.json({ error: "projectId required" }, 400);
  const rclient = await getRedisClient();
  const now = Math.floor(Date.now() / 60_000) * 60_000;
  const pattern = `counter:${projectId}:*:${now}`;

  try {
    const keys: string[] = [];
    let cursor = "0";
    do {
      const [nextCursor, batch] = await rclient.scan(cursor, "MATCH", pattern, "COUNT", 200);
      cursor = nextCursor;
      keys.push(...batch);
    } while (cursor !== "0");

    const events: Array<{ event: string; count: number }> = [];
    for (const key of keys) {
      const parts = key.split(":");
      const event = parts[2];
      const count = parseInt((await rclient.get(key)) || "0", 10);
      if (count > 0) events.push({ event, count });
    }
    queryTotal.inc();
    return c.json({ timestamp: now, events });
  } catch (err) {
    logger.error({ error: err }, "Live query failed");
    return c.json({ error: "query failed" }, 500);
  }
});

function parseWindow(w: string): number {
  const match = w.match(/^(\d+)([smhd])$/);
  if (!match) return 604800;
  const n = parseInt(match[1], 10);
  if (n <= 0) return 604800;
  switch (match[2]) {
    case "s": return n;
    case "m": return n * 60;
    case "h": return n * 3600;
    case "d": return n * 86400;
    default: return 604800;
  }
}

const port = parseInt(process.env.PORT || "3003");
export default { port, fetch: app.fetch };

async function start() {
  initTracing({ serviceName: "query-api-service" });
  pgPool = new Pool({
    host: process.env.POSTGRES_HOST || "localhost",
    port: parseInt(process.env.POSTGRES_PORT || "5432"),
    user: process.env.POSTGRES_USER || "catalyst",
    password: process.env.POSTGRES_PASSWORD || "catalyst",
    database: process.env.POSTGRES_DB || "catalyst",
    max: 10,
  });

  await getRedisClient();
  logger.info({ port }, "Query API service running");
}

start().catch((err) => {
  logger.error({ error: err }, "Fatal error");
  process.exit(1);
});
