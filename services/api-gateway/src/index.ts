import { Hono } from "hono";
import { jwtVerify } from "jose";
import { connectRedis, connectNewRedis } from "@catalyst/redis";
import type { RedisClient } from "@catalyst/redis";
import { createLogger, flushLogs } from "@catalyst/logger";
import crypto from "crypto";
import type { ContentfulStatusCode } from "hono/utils/http-status";
import { initTracing, startSpan, shutdownTracing } from "@catalyst/tracing";
import { createCounter, metricsHandler } from "@catalyst/metrics";

const logger = createLogger({ name: "api-gateway" });

const requestsTotal = createCounter({ name: "gateway_requests_total", help: "Total gateway requests" });
const wsConnectionsTotal = createCounter({ name: "gateway_ws_connections_total", help: "Total WebSocket connections" });

type Variables = { userId: string; orgId: string };
interface WSClient { projectId: string; token: string }

const app = new Hono<{ Variables: Variables }>();
let redis: RedisClient | null = null;
let server: ReturnType<typeof Bun.serve> | null = null;
let subRedis: RedisClient | null = null;
const JWT_SECRET = new TextEncoder().encode(process.env.JWT_SECRET || "dev-secret-change-in-prod");
const MANAGEMENT_SERVICE = process.env.MANAGEMENT_SERVICE_URL || "http://localhost:3001";
const INGEST_SERVICE = process.env.INGEST_SERVICE_URL || "http://localhost:3004";
const QUERY_SERVICE = process.env.QUERY_SERVICE_URL || "http://localhost:3003";

const RATE_LIMIT_WINDOW = 60;
const RATE_LIMIT_MAX = 1000;

const rooms = new Map<string, Set<import("bun").ServerWebSocket<WSClient>>>();

function joinRoom(ws: import("bun").ServerWebSocket<WSClient>, projectId: string) {
  if (!rooms.has(projectId)) rooms.set(projectId, new Set());
  rooms.get(projectId)!.add(ws);
}

function leaveRoom(ws: import("bun").ServerWebSocket<WSClient>, projectId: string) {
  const room = rooms.get(projectId);
  if (!room) return;
  room.delete(ws);
  if (room.size === 0) rooms.delete(projectId);
}

function broadcast(projectId: string, data: string) {
  const room = rooms.get(projectId);
  if (!room || room.size === 0) return;
  for (const ws of room) {
    if (ws.readyState === WebSocket.OPEN) ws.send(data);
  }
}

async function getRedisClient() {
  if (!redis) redis = await connectRedis();
  return redis;
}

async function shutdown() {
  logger.info("Shutting down...");
  server?.stop();
  if (subRedis) await subRedis.quit();
  await redis?.quit();
  await shutdownTracing();
  await flushLogs();
  process.exit(0);
}

process.on("SIGTERM", shutdown);
process.on("SIGINT", shutdown);

async function verifyJWT(token: string) {
  try {
    const { payload } = await jwtVerify(token, JWT_SECRET);
    return payload;
  } catch {
    return null;
  }
}

async function rateLimit(key: string, limit: number = RATE_LIMIT_MAX): Promise<{ allowed: boolean; remaining: number }> {
  const rclient = await getRedisClient();
  const now = Math.floor(Date.now() / 1000);
  const windowStart = Math.floor(now / RATE_LIMIT_WINDOW) * RATE_LIMIT_WINDOW;
  const windowKey = `ratelimit:${key}:${windowStart}`;

  const count = parseInt((await rclient.get(windowKey)) || "0", 10);
  if (count >= limit) return { allowed: false, remaining: 0 };

  await rclient.incr(windowKey);
  if (count === 0) await rclient.expire(windowKey, RATE_LIMIT_WINDOW + 1);
  return { allowed: true, remaining: limit - count - 1 };
}

app.post("/track", async (c) => {
  requestsTotal.inc();
  const auth = c.req.header("Authorization") || "";
  if (!auth.startsWith("Bearer pk_live_")) return c.json({ error: "missing or invalid API key" }, 401);
  const apiKey = auth.slice(7);
  const keyHash = crypto.createHash("sha256").update(apiKey).digest("hex").slice(0, 16);

  const { allowed, remaining } = await rateLimit(`key:${keyHash}`, RATE_LIMIT_MAX);
  if (!allowed) return c.json({ error: "rate limit exceeded" }, 429);

  const projectRes = await fetch(`${MANAGEMENT_SERVICE}/internal/keys/${apiKey}/validate`, { method: "POST" });
  const projectData = await projectRes.json() as { valid: boolean; projectId?: string; orgId?: string };
  if (!projectData.valid) return c.json({ error: "invalid API key" }, 401);

  const body = await c.req.json() as Record<string, unknown>;
  body.projectId = projectData.projectId;

  const clientIp = c.req.header("X-Forwarded-For") || c.req.header("X-Real-IP") || "";
  if (clientIp) {
    if (!body.properties) body.properties = {};
    (body.properties as Record<string, unknown>).clientIp = clientIp;
  }

  const headers: Record<string, string> = { "Content-Type": "application/json" };
  if (clientIp) headers["X-Forwarded-For"] = clientIp;

  const res = await fetch(`${INGEST_SERVICE}/track`, { method: "POST", headers, body: JSON.stringify(body) });
  const data = await res.json();
  const resp = c.json(data, res.status as ContentfulStatusCode);
  resp.headers.set("X-RateLimit-Remaining", String(remaining));
  return resp;
});

app.post("/auth/:path*", async (c) => {
  requestsTotal.inc();
  const path = c.req.path.slice("/auth".length);
  const body = await c.req.text();
  const res = await fetch(`${MANAGEMENT_SERVICE}/auth${path}`, {
    method: c.req.method, headers: { "Content-Type": "application/json" }, body: body || undefined,
  });
  return c.json(await res.json(), res.status as ContentfulStatusCode);
});

app.post("/projects/:path*", async (c) => {
  requestsTotal.inc();
  const path = c.req.path.slice("/projects".length);
  const auth = c.req.header("Authorization") || "";
  const token = auth.startsWith("Bearer ") ? auth.slice(7) : "";
  if (token && !token.startsWith("pk_live_")) {
    const payload = await verifyJWT(token);
    if (!payload) return c.json({ error: "invalid token" }, 401);
  }
  const body = await c.req.text();
  const res = await fetch(`${MANAGEMENT_SERVICE}/projects${path}`, {
    method: c.req.method, headers: { "Content-Type": "application/json", Authorization: auth }, body: body || undefined,
  });
  return c.json(await res.json(), res.status as ContentfulStatusCode);
});

app.get("/projects/:path*", async (c) => {
  requestsTotal.inc();
  const path = c.req.path.slice("/projects".length);
  const auth = c.req.header("Authorization") || "";
  const res = await fetch(`${MANAGEMENT_SERVICE}/projects${path}`, {
    method: "GET", headers: { "Content-Type": "application/json", Authorization: auth },
  });
  return c.json(await res.json(), res.status as ContentfulStatusCode);
});

app.use("/admin/*", async (c, next) => {
  const auth = c.req.header("Authorization") || "";
  if (!auth.startsWith("Bearer ")) return c.json({ error: "unauthorized" }, 401);
  const token = auth.slice(7);
  if (token.startsWith("pk_live_")) return c.json({ error: "API key not allowed here" }, 401);
  const payload = await verifyJWT(token);
  if (!payload) return c.json({ error: "invalid token" }, 401);
  c.set("userId", payload.sub as string);
  c.set("orgId", payload.orgId as string);
  await next();
});

app.all("/admin/:path*", async (c) => {
  requestsTotal.inc();
  const path = c.req.path.slice("/admin".length);
  const body = ["GET", "HEAD"].includes(c.req.method) ? undefined : await c.req.text();
  const res = await fetch(`${MANAGEMENT_SERVICE}/admin${path}`, {
    method: c.req.method,
    headers: { "Content-Type": "application/json", Authorization: c.req.header("Authorization") || "", "X-User-Id": c.var.userId || "", "X-Org-Id": c.var.orgId || "" },
    body: body || undefined,
  });
  return c.json(await res.json(), res.status as ContentfulStatusCode);
});

app.use("/api/*", async (c, next) => {
  const auth = c.req.header("Authorization") || "";
  if (!auth.startsWith("Bearer ")) return c.json({ error: "unauthorized" }, 401);
  const token = auth.slice(7);
  if (token.startsWith("pk_live_")) return c.json({ error: "API key not allowed here" }, 401);
  const payload = await verifyJWT(token);
  if (!payload) return c.json({ error: "invalid token" }, 401);
  c.set("userId", payload.sub as string);
  c.set("orgId", payload.orgId as string);
  await next();
});

app.get("/api/*", async (c) => {
  requestsTotal.inc();
  const path = c.req.path.slice("/api".length);
  const userId = c.var.userId || "";
  const orgId = c.var.orgId || "";
  const res = await fetch(
    `${QUERY_SERVICE}${path}${c.req.url.includes("?") ? "?" + new URL(c.req.url).searchParams.toString() : ""}`,
    { method: "GET", headers: { Authorization: c.req.header("Authorization") || "", "X-User-Id": userId, "X-Org-Id": orgId } },
  );
  return c.json(await res.json(), res.status as ContentfulStatusCode);
});

app.post("/api/*", async (c) => {
  requestsTotal.inc();
  const path = c.req.path.slice("/api".length);
  const userId = c.var.userId || "";
  const orgId = c.var.orgId || "";
  const res = await fetch(`${QUERY_SERVICE}${path}`, {
    method: "POST", headers: { "Content-Type": "application/json", Authorization: c.req.header("Authorization") || "", "X-User-Id": userId, "X-Org-Id": orgId },
    body: await c.req.text(),
  });
  return c.json(await res.json(), res.status as ContentfulStatusCode);
});

app.get("/metrics", async (_c) => metricsHandler());
app.get("/health", (c) => c.json({ status: "ok" }));

const port = parseInt(process.env.PORT || "3000");

async function start() {
  initTracing({ serviceName: "api-gateway" });
  redis = await getRedisClient();

  // Subscribe to Redis live:* channels for WebSocket broadcasting
  subRedis = await connectNewRedis();
  await subRedis.psubscribe("live:*");
  subRedis.on("pmessage", (_pattern: string, channel: string, message: string) => {
    const projectId = channel.split(":").slice(1).join(":");
    broadcast(projectId, message);
  });

  server = Bun.serve<WSClient>({
    port,
    async fetch(req, srv) {
      const url = new URL(req.url);

      if (url.pathname === "/live") {
        requestsTotal.inc();

        const token = url.searchParams.get("token");
        const projectId = url.searchParams.get("projectId");

        if (!token || !projectId) {
          return new Response(JSON.stringify({ error: "token and projectId required" }), { status: 400, headers: { "Content-Type": "application/json" } });
        }

        const payload = await verifyJWT(token);
        if (!payload) {
          return new Response(JSON.stringify({ error: "invalid token" }), { status: 401, headers: { "Content-Type": "application/json" } });
        }

        const upgraded = srv.upgrade(req, { data: { projectId, token } });
        if (!upgraded) return new Response(JSON.stringify({ error: "upgrade failed" }), { status: 400, headers: { "Content-Type": "application/json" } });
        return;
      }

      return app.fetch(req);
    },
    websocket: {
      open(ws) {
        const { projectId } = ws.data;
        const span = startSpan("ws.connect", { "ws.projectId": projectId });
        span.end();

        wsConnectionsTotal.inc();
        joinRoom(ws, projectId);
        logger.info({ projectId, roomSize: rooms.get(projectId)?.size }, "WebSocket connected");
        ws.send(JSON.stringify({ type: "connected", projectId }));
      },
      message() {},
      close(ws) {
        const { projectId } = ws.data;
        leaveRoom(ws, projectId);
        logger.info({ projectId }, "WebSocket disconnected");
      },
    },
  });

  logger.info({ port }, "API Gateway running");
}

start().catch((err) => {
  logger.error({ error: err }, "Fatal error");
  process.exit(1);
});
