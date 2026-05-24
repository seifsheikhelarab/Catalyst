import { connectRedis } from "@catalyst/redis";
import { createLogger } from "@catalyst/logger";
import { jwtVerify } from "jose";
import type { ServerWebSocket } from "bun";
import { initTracing, startSpan } from "@catalyst/tracing";
import { createCounter, metricsHandler } from "@catalyst/metrics";

const logger = createLogger({ name: "websocket-service" });

const connectionsTotal = createCounter({ name: "ws_connections_total", help: "Total WebSocket connections" });

const JWT_SECRET = new TextEncoder().encode(process.env.JWT_SECRET || "dev-secret-change-in-prod");
const PORT = parseInt(process.env.PORT || "3005");

interface ClientData {
  projectId: string;
  token: string;
}

const rooms = new Map<string, Set<ServerWebSocket<ClientData>>>();

function joinRoom(ws: ServerWebSocket<ClientData>, projectId: string) {
  if (!rooms.has(projectId)) rooms.set(projectId, new Set());
  rooms.get(projectId)!.add(ws);
}

function leaveRoom(ws: ServerWebSocket<ClientData>, projectId: string) {
  const room = rooms.get(projectId);
  if (!room) return;
  room.delete(ws);
  if (room.size === 0) rooms.delete(projectId);
}

function broadcast(projectId: string, data: string) {
  const room = rooms.get(projectId);
  if (!room || room.size === 0) return;
  for (const ws of room) {
    if (ws.readyState === WebSocket.OPEN) {
      ws.send(data);
    }
  }
}

async function start() {
  initTracing({ serviceName: "websocket-service" });
  const subRedis = await connectRedis();
  await subRedis.psubscribe("live:*");
  subRedis.on("pmessage", (_pattern: string, channel: string, message: string) => {
    const projectId = channel.split(":").slice(1).join(":");
    broadcast(projectId, message);
  });

  const server = Bun.serve<ClientData>({
    port: PORT,
    async fetch(req, server) {
      const url = new URL(req.url);

      if (url.pathname === "/metrics") {
        const metricsRes = await metricsHandler();
        return new Response(await metricsRes.text(), {
          headers: { "Content-Type": metricsRes.headers.get("Content-Type") || "text/plain" },
        });
      }

      if (url.pathname === "/health") {
        return Response.json({ status: "ok" });
      }

      if (url.pathname !== "/live") {
        return Response.json({ error: "not found" }, { status: 404 });
      }

      const token = url.searchParams.get("token");
      const projectId = url.searchParams.get("projectId");

      if (!token || !projectId) {
        return Response.json({ error: "token and projectId required" }, { status: 400 });
      }

      const upgraded = server.upgrade(req, { data: { projectId, token } });
      if (!upgraded) {
        return Response.json({ error: "upgrade failed" }, { status: 400 });
      }
    },
    websocket: {
      async open(ws) {
        const { projectId, token } = ws.data;

        if (!token) {
          ws.close(4001, "token required");
          return;
        }

        try {
          const { payload } = await jwtVerify(token, JWT_SECRET);
          if (!payload.sub) {
            ws.close(4001, "invalid token");
            return;
          }
        } catch {
          ws.close(4001, "invalid token");
          return;
        }

        connectionsTotal.inc();
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

  logger.info({ port: PORT }, "WebSocket service running");

  async function shutdown() {
    logger.info("Shutting down...");
    server.stop();
    await subRedis.quit();
    process.exit(0);
  }

  process.on("SIGTERM", shutdown);
  process.on("SIGINT", shutdown);
}

start().catch((err) => {
  logger.error({ error: err }, "Fatal error");
  process.exit(1);
});
