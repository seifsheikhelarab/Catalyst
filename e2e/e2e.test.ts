import { describe, it, expect, beforeAll } from "bun:test";
import request from "supertest";

const GATEWAY = "http://localhost:3000";
const INGEST = "http://localhost:3004";

let accessToken: string;
let refreshToken: string;
let userId: string;
let orgId: string;
let projectId: string;
let apiKey: string;

const email = `e2e-${Date.now()}@test.com`;
const password = "testpass123";

beforeAll(async () => {
  const res = await request(GATEWAY).get("/health");
  if (res.status !== 200) {
    throw new Error("API gateway not running — start with `docker compose up -d`");
  }
});

describe("A — Registration & Auth", () => {
  it("registers a new user", async () => {
    const res = await request(GATEWAY)
      .post("/auth/register")
      .send({ email, password });
    expect(res.status).toBe(201);
    expect(res.body).toHaveProperty("accessToken");
    expect(res.body).toHaveProperty("refreshToken");
    expect(res.body.user).toHaveProperty("id");
    expect(res.body.user).toHaveProperty("orgId");
    expect(res.body.user.email).toBe(email);
    accessToken = res.body.accessToken;
    refreshToken = res.body.refreshToken;
    userId = res.body.user.id;
    orgId = res.body.user.orgId;
  });

  it("fails to register with same email", async () => {
    const res = await request(GATEWAY)
      .post("/auth/register")
      .send({ email, password });
    expect(res.status).toBe(409);
    expect(res.body.error).toBe("email already exists");
  });

  it("logs in", async () => {
    const res = await request(GATEWAY)
      .post("/auth/login")
      .send({ email, password });
    expect(res.status).toBe(200);
    expect(res.body).toHaveProperty("accessToken");
    expect(res.body).toHaveProperty("refreshToken");
    accessToken = res.body.accessToken;
    refreshToken = res.body.refreshToken;
  });

  it("gets current user", async () => {
    const res = await request(GATEWAY)
      .get("/auth/me")
      .set("Authorization", `Bearer ${accessToken}`);
    expect(res.status).toBe(200);
    expect(res.body.id).toBe(userId);
  });

  it("refreshes tokens", async () => {
    const res = await request(GATEWAY)
      .post("/auth/refresh")
      .send({ refreshToken });
    expect(res.status).toBe(200);
    expect(res.body).toHaveProperty("accessToken");
    expect(res.body).toHaveProperty("refreshToken");
    accessToken = res.body.accessToken;
    refreshToken = res.body.refreshToken;
  });

  it("logs out", async () => {
    const res = await request(GATEWAY)
      .post("/auth/logout")
      .send({ refreshToken });
    expect(res.status).toBe(200);
    expect(res.body.ok).toBe(true);
  });
});

describe("B — Project & API Key Management", () => {
  it("creates a project", async () => {
    const res = await request(GATEWAY)
      .post("/projects")
      .set("Authorization", `Bearer ${accessToken}`)
      .send({ name: "E2E Test App", orgId });
    expect(res.status).toBe(201);
    expect(res.body).toHaveProperty("id");
    projectId = res.body.id;
  });

  it("gets project by ID", async () => {
    const res = await request(GATEWAY)
      .get(`/projects/${projectId}`)
      .set("Authorization", `Bearer ${accessToken}`);
    expect(res.status).toBe(200);
    expect(res.body.id).toBe(projectId);
  });

  it("lists projects", async () => {
    const res = await request(GATEWAY)
      .get(`/projects?orgId=${orgId}`)
      .set("Authorization", `Bearer ${accessToken}`);
    expect(res.status).toBe(200);
    expect(Array.isArray(res.body)).toBe(true);
    expect(res.body.length).toBeGreaterThanOrEqual(1);
  });

  it("creates an API key", async () => {
    const res = await request(GATEWAY)
      .post(`/projects/${projectId}/keys`)
      .set("Authorization", `Bearer ${accessToken}`);
    expect(res.status).toBe(201);
    expect(res.body).toHaveProperty("key");
    expect(res.body.key).toStartWith("pk_live_");
    apiKey = res.body.key;
  });

  it("lists API keys", async () => {
    const res = await request(GATEWAY)
      .get(`/projects/${projectId}/keys`)
      .set("Authorization", `Bearer ${accessToken}`);
    expect(res.status).toBe(200);
    expect(Array.isArray(res.body)).toBe(true);
    expect(res.body.length).toBeGreaterThanOrEqual(1);
  });
});

describe("C — Event Schema Management", () => {
  it("creates an event schema for page_view", async () => {
    const res = await request(GATEWAY)
      .post(`/projects/${projectId}/schemas`)
      .set("Authorization", `Bearer ${accessToken}`)
      .send({
        eventName: "page_view",
        schema: {
          type: "object",
          required: ["url"],
          properties: {
            url: { type: "string" },
            referrer: { type: "string" },
          },
        },
      });
    expect(res.status).toBe(201);
    expect(res.body.event_name).toBe("page_view");
  });

  it("creates a strict schema for dlq_trigger (to test DLQ)", async () => {
    const res = await request(GATEWAY)
      .post(`/projects/${projectId}/schemas`)
      .set("Authorization", `Bearer ${accessToken}`)
      .send({
        eventName: "dlq_trigger",
        schema: {
          type: "object",
          required: ["required_field"],
          properties: {
            required_field: { type: "string" },
          },
        },
      });
    expect(res.status).toBe(201);
    expect(res.body.event_name).toBe("dlq_trigger");
  });

  it("lists schemas", async () => {
    const res = await request(GATEWAY)
      .get(`/projects/${projectId}/schemas`)
      .set("Authorization", `Bearer ${accessToken}`);
    expect(res.status).toBe(200);
    expect(Array.isArray(res.body)).toBe(true);
    expect(res.body.length).toBeGreaterThanOrEqual(2);
  });

  it("gets schema by event name", async () => {
    const res = await request(GATEWAY)
      .get(`/projects/${projectId}/schemas/page_view`)
      .set("Authorization", `Bearer ${accessToken}`);
    expect(res.status).toBe(200);
    expect(res.body.event_name).toBe("page_view");
  });
});

describe("D — Event Pipeline", () => {
  it("sends a valid event through the gateway", async () => {
    const res = await request(GATEWAY)
      .post("/track")
      .set("Authorization", `Bearer ${apiKey}`)
      .send({
        event: "page_view",
        userId: "user_e2e",
        properties: {
          url: "/pricing",
          userAgent: "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
        },
        timestamp: Date.now(),
      });
    expect(res.status).toBe(202);
  });
});

describe("E — Pipeline Verification", () => {
  it("live counter returns data for the project", async () => {
    await new Promise((r) => setTimeout(r, 2000));

    const res = await request(GATEWAY)
      .get(`/api/projects/${projectId}/events/live`)
      .set("Authorization", `Bearer ${accessToken}`);
    expect(res.status).toBe(200);
  });
});

describe("F — Query API", () => {
  it("fetches metrics", async () => {
    const from = new Date(Date.now() - 3600_000).toISOString();
    const to = new Date().toISOString();
    const res = await request(GATEWAY)
      .get(`/api/projects/${projectId}/metrics?event=page_view&from=${from}&to=${to}`)
      .set("Authorization", `Bearer ${accessToken}`);
    expect(res.status).toBe(200);
  });

  it("fetches users", async () => {
    const res = await request(GATEWAY)
      .get(`/api/projects/${projectId}/users?limit=10`)
      .set("Authorization", `Bearer ${accessToken}`);
    expect(res.status).toBe(200);
    expect(Array.isArray(res.body)).toBe(true);
  });

  it("fetches funnels", async () => {
    const res = await request(GATEWAY)
      .get(`/api/projects/${projectId}/funnels?steps=["page_view","signup"]&window=7d`)
      .set("Authorization", `Bearer ${accessToken}`);
    expect(res.status).toBe(200);
  });

  it("fetches retention", async () => {
    const res = await request(GATEWAY)
      .get(`/api/projects/${projectId}/retention?cohortEvent=page_view&returnEvent=page_view&periods=4`)
      .set("Authorization", `Bearer ${accessToken}`);
    expect(res.status).toBe(200);
  });
});

describe("G — DLQ Flow", () => {
  it("sends an event directly to ingest that will be DLQed (bypasses gateway)", async () => {
    const res = await request(INGEST)
      .post("/track")
      .send({
        event: "dlq_trigger",
        projectId,
        timestamp: Date.now(),
      });
    expect(res.status).toBe(202);
  });

  it("lists DLQ events with at least 1 entry", async () => {
    await new Promise((r) => setTimeout(r, 8000));

    const res = await request(GATEWAY)
      .get("/admin/dlq")
      .set("Authorization", `Bearer ${accessToken}`);
    expect(res.status).toBe(200);
    expect(res.body).toHaveProperty("total");
    expect(res.body.total).toBeGreaterThan(0);
    expect(Array.isArray(res.body.events)).toBe(true);

    const dlqEvent = res.body.events[0];
    expect(dlqEvent).toHaveProperty("reason");
    expect(dlqEvent.reason).toMatch(/required_field|Missing required/i);
  });
});

describe("H — Duplicate Dedup", () => {
  it("rejects duplicate event", async () => {
    const payload = {
      event: "test_dedup",
      userId: "dedup_user",
      properties: { url: "/test" },
      timestamp: Date.now(),
    };

    const first = await request(GATEWAY)
      .post("/track")
      .set("Authorization", `Bearer ${apiKey}`)
      .send(payload);
    expect(first.status).toBe(202);

    const second = await request(GATEWAY)
      .post("/track")
      .set("Authorization", `Bearer ${apiKey}`)
      .send(payload);
    expect(second.status).toBe(202);
    expect(second.body.status).toBe("duplicate");
  });
});
