import { Hono } from "hono";
import { jwtVerify } from "jose";
import { connectRedis } from "@catalyst/redis";
import type { RedisClient } from "@catalyst/redis";
import { createLogger, flushLogs } from "@catalyst/logger";
import crypto from "crypto";
import type { ContentfulStatusCode } from "hono/utils/http-status";
import { initTracing, startSpan, shutdownTracing } from "@catalyst/tracing";
import { createCounter, metricsHandler } from "@catalyst/metrics";
import { createOpenApiRoutes } from "./openapi";

const logger = createLogger({ name: "api-gateway" });

const requestsTotal = createCounter({ name: "gateway_requests_total", help: "Total gateway requests" });
const wsConnectionsTotal = createCounter({ name: "gateway_ws_connections_total", help: "Total WebSocket connections" });

type Variables = {
  userId: string;
  orgId: string;
};

const app = new Hono<{ Variables: Variables }>();
let redis: RedisClient | null = null;
let server: ReturnType<typeof Bun.serve> | null = null;
const JWT_SECRET = new TextEncoder().encode(process.env.JWT_SECRET || "dev-secret-change-in-prod");
const PROJECT_SERVICE = process.env.PROJECT_SERVICE_URL || "http://localhost:3001";
const AUTH_SERVICE = process.env.AUTH_SERVICE_URL || "http://localhost:3002";
const INGEST_SERVICE = process.env.INGEST_SERVICE_URL || "http://localhost:3004";
const QUERY_SERVICE = process.env.QUERY_SERVICE_URL || "http://localhost:3003";
const DLQ_SERVICE = process.env.DLQ_SERVICE_URL || "http://localhost:3006";

const RATE_LIMIT_WINDOW = 60;
const RATE_LIMIT_MAX = 1000;

const VALID_STATUS_CODES = new Set([200, 201, 202, 301, 302, 400, 401, 403, 404, 405, 409, 422, 429, 500, 502, 503]);

function safeStatus(code: number): ContentfulStatusCode {
  return (VALID_STATUS_CODES.has(code) ? code : 500) as ContentfulStatusCode;
}

async function getRedisClient() {
  if (!redis) redis = await connectRedis();
  return redis;
}

async function shutdown() {
  logger.info("Shutting down...");
  server?.stop();
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
  if (count >= limit) {
    return { allowed: false, remaining: 0 };
  }
  await rclient.incr(windowKey);
  if (count === 0) {
    await rclient.expire(windowKey, RATE_LIMIT_WINDOW + 1);
  }
  return { allowed: true, remaining: limit - count - 1 };
}

app.post("/track", async (c) => {
  requestsTotal.inc();
  const auth = c.req.header("Authorization") || "";
  if (!auth.startsWith("Bearer pk_live_")) {
    return c.json({ error: "missing or invalid API key" }, 401);
  }
  const apiKey = auth.slice(7);
  const keyHash = crypto.createHash("sha256").update(apiKey).digest("hex").slice(0, 16);

  const { allowed, remaining } = await rateLimit(`key:${keyHash}`, RATE_LIMIT_MAX);
  if (!allowed) {
    return c.json({ error: "rate limit exceeded" }, 429);
  }

  const projectRes = await fetch(`${PROJECT_SERVICE}/internal/keys/${apiKey}/validate`, { method: "POST" });
  const projectData: { valid: boolean; projectId?: string; orgId?: string } = await projectRes.json();
  if (!projectData.valid) {
    return c.json({ error: "invalid API key" }, 401);
  }

  const body: Record<string, unknown> = await c.req.json();
  body.projectId = projectData.projectId;

  // Forward client IP for GeoIP enrichment
  const clientIp = c.req.header("X-Forwarded-For") || c.req.header("X-Real-IP") || "";
  if (clientIp) {
    if (!body.properties) body.properties = {};
    (body.properties as Record<string, unknown>).clientIp = clientIp;
  }

  const headers: Record<string, string> = { "Content-Type": "application/json" };
  if (clientIp) headers["X-Forwarded-For"] = clientIp;

  const res = await fetch(`${INGEST_SERVICE}/track`, {
    method: "POST",
    headers,
    body: JSON.stringify(body),
  });
  const data = await res.json();
  const resp = c.json(data, safeStatus(res.status));
  resp.headers.set("X-RateLimit-Remaining", String(remaining));
  return resp;
});

app.post("/auth/:path*", async (c) => {
  requestsTotal.inc();
  const path = c.req.path.slice("/auth".length);
  const body = await c.req.text();
  const res = await fetch(`${AUTH_SERVICE}/auth${path}`, {
    method: c.req.method,
    headers: { "Content-Type": "application/json" },
    body: body || undefined,
  });
  return c.json(await res.json(), safeStatus(res.status));
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
  const res = await fetch(`${PROJECT_SERVICE}/projects${path}`, {
    method: c.req.method,
    headers: { "Content-Type": "application/json", Authorization: auth },
    body: body || undefined,
  });
  return c.json(await res.json(), safeStatus(res.status));
});

app.get("/projects/:path*", async (c) => {
  requestsTotal.inc();
  const path = c.req.path.slice("/projects".length);
  const auth = c.req.header("Authorization") || "";
  const res = await fetch(`${PROJECT_SERVICE}/projects${path}`, {
    method: "GET",
    headers: { "Content-Type": "application/json", Authorization: auth },
  });
  return c.json(await res.json(), safeStatus(res.status));
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
  const res = await fetch(`${DLQ_SERVICE}/admin${path}`, {
    method: c.req.method,
    headers: {
      "Content-Type": "application/json",
      Authorization: c.req.header("Authorization") || "",
      "X-User-Id": c.var.userId || "",
      "X-Org-Id": c.var.orgId || "",
    },
    body: body || undefined,
  });
  return c.json(await res.json(), safeStatus(res.status));
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
    {
      method: "GET",
      headers: {
        Authorization: c.req.header("Authorization") || "",
        "X-User-Id": userId,
        "X-Org-Id": orgId,
      },
    },
  );
  return c.json(await res.json(), safeStatus(res.status));
});

app.post("/api/*", async (c) => {
  requestsTotal.inc();
  const path = c.req.path.slice("/api".length);
  const userId = c.var.userId || "";
  const orgId = c.var.orgId || "";
  const res = await fetch(`${QUERY_SERVICE}${path}`, {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
      Authorization: c.req.header("Authorization") || "",
      "X-User-Id": userId,
      "X-Org-Id": orgId,
    },
    body: await c.req.text(),
  });
  return c.json(await res.json(), safeStatus(res.status));
});

app.get("/metrics", async (_c) => {
  return metricsHandler();
});

app.get("/health", (c) => c.json({ status: "ok" }));

createOpenApiRoutes(app);

const port = parseInt(process.env.PORT || "3000");
const WS_SERVICE_URL = process.env.WS_SERVICE_URL || "http://localhost:3005";

async function start() {
  initTracing({ serviceName: "api-gateway" });
  redis = await getRedisClient();

  server = Bun.serve<{ projectId: string; token: string; upstream: WebSocket | null }>({
    port,
    async fetch(req, srv) {
      const url = new URL(req.url);

      if (url.pathname === "/live") {
        requestsTotal.inc();

        const token = url.searchParams.get("token");
        const projectId = url.searchParams.get("projectId");

        if (!token || !projectId) {
          return new Response(JSON.stringify({ error: "token and projectId required" }), {
            status: 400,
            headers: { "Content-Type": "application/json" },
          });
        }

        const payload = await verifyJWT(token);
        if (!payload) {
          return new Response(JSON.stringify({ error: "invalid token" }), {
            status: 401,
            headers: { "Content-Type": "application/json" },
          });
        }

        const upgraded = srv.upgrade(req, { data: { projectId, token, upstream: null } });
        if (!upgraded) {
          return new Response(JSON.stringify({ error: "upgrade failed" }), {
            status: 400,
            headers: { "Content-Type": "application/json" },
          });
        }
        return;
      }

      return app.fetch(req);
    },
    websocket: {
      open(ws) {
        const { projectId, token } = ws.data;
        const wsUrl = `${WS_SERVICE_URL.replace(/^http/, "ws")}/live?token=${encodeURIComponent(token)}&projectId=${encodeURIComponent(projectId)}`;

        const upstream = new WebSocket(wsUrl);
        ws.data.upstream = upstream;

        // Propagate trace context on WS connection
        const span = startSpan("ws.proxy.connect", { "ws.projectId": projectId });

        upstream.onopen = () => {
          wsConnectionsTotal.inc();
          span.end();
        };

        upstream.onerror = () => {
          span.end();
        };

        upstream.onmessage = (event) => {
          if (ws.readyState === WebSocket.OPEN) {
            ws.send(event.data);
          }
        };

        upstream.onerror = () => {
          ws.close(1011, "upstream error");
        };

        upstream.onclose = () => {
          ws.close();
        };
      },
      message(ws, message) {
        const up = ws.data.upstream;
        if (up && up.readyState === WebSocket.OPEN) {
          up.send(message);
        }
      },
      close(ws) {
        ws.data.upstream?.close();
      },
    },
  });

  logger.info({ port }, "API Gateway running");
}

start().catch((err) => {
  logger.error({ error: err }, "Fatal error");
  process.exit(1);
});
