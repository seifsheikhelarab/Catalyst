import { Hono } from "hono";
import pg from "pg";
import bcrypt from "bcrypt";
import { SignJWT, jwtVerify } from "jose";
import { connectRedis } from "@catalyst/redis";
import { createLogger, flushLogs } from "@catalyst/logger";
import { initTracing, startSpan, shutdownTracing } from "@catalyst/tracing";
import { createCounter, metricsHandler } from "@catalyst/metrics";
import type { RedisClient } from "@catalyst/redis";
import crypto from "crypto";

const { Pool } = pg;
const logger = createLogger({ name: "auth-service" });

const requestsTotal = createCounter({ name: "auth_requests_total", help: "Total auth service requests" });
const loginSuccess = createCounter({ name: "auth_login_success_total", help: "Successful logins" });
const loginFailed = createCounter({ name: "auth_login_failed_total", help: "Failed login attempts" });

const ACCESS_TOKEN_TTL = 15 * 60;
const REFRESH_TOKEN_TTL = 7 * 24 * 60 * 60;

const app = new Hono();
let redis: RedisClient | null = null;
let pool: pg.Pool;

const JWT_SECRET = new TextEncoder().encode(process.env.JWT_SECRET || "dev-secret-change-in-prod");

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
  await shutdownTracing();
  await flushLogs();
  process.exit(0);
}

process.on("SIGTERM", shutdown);
process.on("SIGINT", shutdown);

async function signToken(
  payload: { sub: string; orgId: string; type: "access" | "refresh" },
  expiresIn: number,
) {
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
    await client.query("INSERT INTO orgs (id, name) VALUES ($1, $2)", [
      orgId,
      orgName || email.split("@")[0],
    ]);
    await client.query(
      "INSERT INTO users (id, org_id, email, password_hash) VALUES ($1, $2, $3, $4)",
      [userId, orgId, email, passwordHash],
    );
    await client.query("COMMIT");

    const accessToken = await signToken({ sub: userId, orgId, type: "access" }, ACCESS_TOKEN_TTL);
    const refreshToken = await signToken(
      { sub: userId, orgId, type: "refresh" },
      REFRESH_TOKEN_TTL,
    );

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
    const result = await client.query(
      "SELECT id, org_id, email, password_hash FROM users WHERE email = $1",
      [email],
    );
    if (result.rows.length === 0) {
      loginFailed.inc();
      return c.json({ error: "invalid credentials" }, 401);
    }

    const user = result.rows[0];
    const match = await bcrypt.compare(password, user.password_hash);
    if (!match) {
      loginFailed.inc();
      return c.json({ error: "invalid credentials" }, 401);
    }

    loginSuccess.inc();

    const accessToken = await signToken(
      { sub: user.id, orgId: user.org_id, type: "access" },
      ACCESS_TOKEN_TTL,
    );
    const refreshToken = await signToken(
      { sub: user.id, orgId: user.org_id, type: "refresh" },
      REFRESH_TOKEN_TTL,
    );

    const rclient = await getRedisClient();
    await rclient.setex(`refresh:${refreshToken}`, REFRESH_TOKEN_TTL, user.id);

    return c.json({
      accessToken,
      refreshToken,
      user: { id: user.id, email: user.email, orgId: user.org_id },
    });
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
  if (!payload || payload.type !== "refresh")
    return c.json({ error: "invalid refresh token" }, 401);

  const rclient = await getRedisClient();
  const stored = await rclient.get(`refresh:${refreshToken}`);
  if (!stored || stored !== payload.sub) return c.json({ error: "token revoked" }, 401);

  // Revoke old refresh token (rotation for security)
  await rclient.del(`refresh:${refreshToken}`);

  const userId = payload.sub as string;
  const orgId = payload.orgId as string;
  const newAccessToken = await signToken({ sub: userId, orgId, type: "access" }, ACCESS_TOKEN_TTL);
  const newRefreshToken = await signToken({ sub: userId, orgId, type: "refresh" }, REFRESH_TOKEN_TTL);

  // Store new refresh token
  await rclient.setex(`refresh:${newRefreshToken}`, REFRESH_TOKEN_TTL, userId);

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
    const result = await client.query("SELECT id, org_id, email FROM users WHERE id = $1", [
      payload.sub,
    ]);
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

app.get("/metrics", (_c) => metricsHandler());
app.get("/health", (c) => c.json({ status: "ok" }));

const port = parseInt(process.env.PORT || "3002");
export default { port, fetch: app.fetch };

async function start() {
  initTracing({ serviceName: "auth-service" });

  pool = new Pool({
    host: process.env.POSTGRES_HOST || "localhost",
    port: parseInt(process.env.POSTGRES_PORT || "5432"),
    user: process.env.POSTGRES_USER || "catalyst",
    password: process.env.POSTGRES_PASSWORD || "catalyst",
    database: process.env.POSTGRES_DB || "catalyst",
    max: 20,
  });

  await getRedisClient();
  logger.info({ port }, "Auth service running");
}

start().catch((err) => {
  logger.error({ error: err }, "Fatal error");
  process.exit(1);
});
