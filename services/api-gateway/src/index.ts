import { Hono } from "hono";
import { jwtVerify } from "jose";
import { connectRedis } from "@catalyst/redis";
import { createLogger } from "@catalyst/logger";
import crypto from "crypto";

const logger = createLogger({ name: "api-gateway" });

const app = new Hono();
let redis: any;
const JWT_SECRET = new TextEncoder().encode(process.env.JWT_SECRET || "dev-secret-change-in-prod");
const PROJECT_SERVICE = process.env.PROJECT_SERVICE_URL || "http://localhost:3001";
const AUTH_SERVICE = process.env.AUTH_SERVICE_URL || "http://localhost:3002";
const INGEST_SERVICE = process.env.INGEST_SERVICE_URL || "http://localhost:3004";
const QUERY_SERVICE = process.env.QUERY_SERVICE_URL || "http://localhost:3003";

const RATE_LIMIT_WINDOW = 60;
const RATE_LIMIT_MAX = 1000;

async function getRedisClient() {
  if (!redis) redis = await connectRedis();
  return redis;
}

async function shutdown() {
  logger.info("Shutting down...");
  await redis?.quit();
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

async function rateLimit(
  key: string,
  limit: number = RATE_LIMIT_MAX,
): Promise<{ allowed: boolean; remaining: number }> {
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

async function proxy(url: string, req: Request): Promise<Response> {
  const options: RequestInit = {
    method: req.method,
    headers: {
      "Content-Type": "application/json",
      "X-Forwarded-For": req.headers.get("x-forwarded-for") || "unknown",
      ...Object.fromEntries(req.headers.entries()),
    },
  };
  if (req.body) {
    options.body = await req.text();
  }
  const proxied = await fetch(url, options);
  return proxied;
}

app.post("/track", async (c) => {
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

  const projectRes = await fetch(`${PROJECT_SERVICE}/internal/keys/${apiKey}/validate`, {
    method: "POST",
  });
  const projectData = await projectRes.json();
  if (!projectData.valid) {
    return c.json({ error: "invalid API key" }, 401);
  }

  c.set("projectId", projectData.projectId);
  c.set("orgId", projectData.orgId);

  const body = await c.req.json();
  body.projectId = projectData.projectId;

  const res = await fetch(`${INGEST_SERVICE}/track`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(body),
  });
  const data = await res.json();
  const resp = c.json(data, res.status);
  resp.headers.set("X-RateLimit-Remaining", String(remaining));
  return resp;
});

app.post("/auth/:path*", async (c) => {
  const path = c.req.path.slice("/auth".length);
  const res = await fetch(`${AUTH_SERVICE}/auth${path}`, {
    method: c.req.method,
    headers: { "Content-Type": "application/json" },
    body: c.req.body ? await c.req.text() : undefined,
  });
  return c.json(await res.json(), res.status);
});

app.post("/projects/:path*", async (c) => {
  const path = c.req.path.slice("/projects".length);
  const auth = c.req.header("Authorization") || "";
  const token = auth.startsWith("Bearer ") ? auth.slice(7) : "";
  if (token && !token.startsWith("pk_live_")) {
    const payload = await verifyJWT(token);
    if (!payload) return c.json({ error: "invalid token" }, 401);
  }
  const res = await fetch(`${PROJECT_SERVICE}/projects${path}`, {
    method: c.req.method,
    headers: {
      "Content-Type": "application/json",
      Authorization: auth,
    },
    body: c.req.body ? await c.req.text() : undefined,
  });
  return c.json(await res.json(), res.status);
});

app.get("/projects/:path*", async (c) => {
  const path = c.req.path.slice("/projects".length);
  const auth = c.req.header("Authorization") || "";
  const res = await fetch(`${PROJECT_SERVICE}/projects${path}`, {
    method: "GET",
    headers: { "Content-Type": "application/json", Authorization: auth },
  });
  return c.json(await res.json(), res.status);
});

app.use("/api/*", async (c, next) => {
  const auth = c.req.header("Authorization") || "";
  if (!auth.startsWith("Bearer ")) return c.json({ error: "unauthorized" }, 401);
  const token = auth.slice(7);
  if (token.startsWith("pk_live_")) return c.json({ error: "API key not allowed here" }, 401);
  const payload = await verifyJWT(token);
  if (!payload) return c.json({ error: "invalid token" }, 401);
  c.set("userId", payload.sub);
  c.set("orgId", payload.orgId);
  await next();
});

app.get("/api/*", async (c) => {
  const path = c.req.path.slice("/api".length);
  const res = await fetch(
    `${QUERY_SERVICE}${path}${c.req.url.includes("?") ? "?" + new URL(c.req.url).searchParams.toString() : ""}`,
    {
      method: "GET",
      headers: {
        Authorization: c.req.header("Authorization") || "",
        "X-User-Id": c.get("userId") || "",
        "X-Org-Id": c.get("orgId") || "",
      },
    },
  );
  return c.json(await res.json(), res.status);
});

app.post("/api/*", async (c) => {
  const path = c.req.path.slice("/api".length);
  const res = await fetch(`${QUERY_SERVICE}${path}`, {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
      Authorization: c.req.header("Authorization") || "",
      "X-User-Id": c.get("userId") || "",
      "X-Org-Id": c.get("orgId") || "",
    },
    body: await c.req.text(),
  });
  return c.json(await res.json(), res.status);
});

app.get("/health", (c) => c.json({ status: "ok" }));

const port = parseInt(process.env.PORT || "3000");
export default { port, fetch: app.fetch };

async function start() {
  redis = await getRedisClient();
  logger.info({ port }, "API Gateway running");
}

start().catch((err) => {
  logger.error({ error: err }, "Fatal error");
  process.exit(1);
});
