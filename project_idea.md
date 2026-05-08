# Catalyst - Real-Time Analytics Pipeline

---

### Core Concept

You're building a **product analytics platform**. Clients embed a tiny SDK (or hit your API) to track events (`page_view`, `button_click`, `purchase`). Your system ingests millions of these events, processes them in real-time, and serves dashboards with live metrics.

The message broker is the **spine** of the entire system — not an afterthought.

---

### Architecture Overview

```
Client SDK
    │
    ▼
[API Gateway]  ←── auth, rate limiting, routing
    │
    ▼
[Ingest Service]  ←── receive & ack fast, no processing here
    │
    ▼ (Kafka topic: raw-events)
[Validation Service]  ←── schema check, drop malformed
    │
    ▼ (Kafka topic: validated-events)
[Enrichment Service]  ←── geo-lookup, UA parsing, session stitching
    │
    ▼ (Kafka topic: enriched-events)
    ├──▶ [Stream Processor]  ←── rolling windows, funnels, retention
    │         │
    │         ▼ (writes to TimescaleDB + Redis)
    │
    └──▶ [Raw Storage Service]  ←── dumps to ClickHouse/Parquet for ad-hoc queries

[Query API Service]  ←── serves dashboard queries from TimescaleDB/ClickHouse
    │
    ▼
[WebSocket Service]  ←── pushes live metric updates to dashboard
    │
    ▼
[Dashboard UI]  ←── (optional, keep it minimal)
```

---

### Services in Detail

---

#### 1. `ingest-service`

**Job:** Receive events as fast as possible and push to Kafka. That's it.

```
POST /track
{
  "event": "page_view",
  "projectId": "proj_abc",
  "userId": "usr_123",
  "properties": {
    "url": "/pricing",
    "referrer": "google.com"
  },
  "timestamp": 1714000000000
}
```

- Validates only that `event` and `projectId` exist (nothing expensive)
- Publishes to `raw-events` Kafka topic
- Returns `202 Accepted` immediately
- **Scales horizontally** — this is your hot path

Key concepts: **back-pressure**, **fire-and-forget**, **idempotency keys** (dedup on `messageId`)

---

#### 2. `validation-service`

**Job:** Kafka consumer on `raw-events`. Validates full schema, filters bots, enforces project-level event schemas.

- Consumes `raw-events`
- Runs Zod schemas per project (fetched from config service / Redis cache)
- Valid → publish to `validated-events`
- Invalid → publish to `dead-letter-events` (for debugging)
- Tracks validation failure rate per project

Key concepts: **consumer groups**, **dead letter queues**, **schema registry**

---

#### 3. `enrichment-service`

**Job:** Kafka consumer on `validated-events`. Adds context that the client didn't send.

Enrichments:

- **Geo:** IP → country, city, region (MaxMind GeoLite2, local lookup, no external API call)
- **Device/OS:** User-Agent parsing → browser, OS, device type
- **Session:** Stitch events into sessions (30-min inactivity = new session) using Redis
- **Identity resolution:** anonymous `deviceId` → known `userId` if they logged in

Publishes to `enriched-events`.

Key concepts: **stateful stream processing** (session stitching in Redis), **identity graphs**

---

#### 4. `stream-processor-service`

**Job:** The brain. Consumes `enriched-events` and maintains real-time aggregates.

Aggregates it maintains:

```
- Event counts (per event, per project, per time bucket: 1min / 1hr / 1day)
- Unique users (HyperLogLog in Redis for approximation at scale)
- Funnel conversion rates (step A → step B within N minutes)
- Retention cohorts (did user who did X on day 0 return on day 7?)
- Session duration averages
```

Storage:

- **Redis** → live counters, current active users, rolling 24h windows
- **TimescaleDB** → time-series aggregates for historical queries

Key concepts: **tumbling vs sliding windows**, **HyperLogLog**, **funnel analysis**, **materialized rollups**

---

#### 5. `raw-storage-service`

**Job:** Dumb consumer. Takes `enriched-events` and writes them raw for ad-hoc SQL queries.

- Batches events (1000 events or 5s, whichever first)
- Writes to **ClickHouse** (columnar, absurdly fast for analytics queries)
- Enables queries like: _"Show me all users who did X then Y then purchased within 1 hour"_

Key concepts: **micro-batching**, **columnar storage**, **OLAP vs OLTP**

---

#### 6. `query-api-service`

**Job:** REST API that the dashboard calls to fetch metrics.

```
GET /projects/:id/metrics?event=page_view&from=2024-01-01&to=2024-01-31&groupBy=day
GET /projects/:id/funnels?steps=["signup","onboard","purchase"]&window=7d
GET /projects/:id/retention?cohortEvent=signup&returnEvent=login&periods=8w
GET /projects/:id/users?filter=country:EG&limit=50
```

- Simple queries → TimescaleDB (pre-aggregated, fast)
- Complex ad-hoc → ClickHouse
- Caches frequent queries in Redis with short TTL

Key concepts: **CQRS** (this service is pure read side), **query planning**, **cache invalidation strategy**

---

#### 7. `websocket-service`

**Job:** Push live updates to connected dashboards without polling.

- Clients connect via WebSocket with their `projectId`
- Stream processor publishes to Redis Pub/Sub on every aggregate update
- WebSocket service subscribes and fans out to connected clients

```
// Client receives every ~1s:
{
  "type": "metric_update",
  "event": "page_view",
  "count_1m": 142,
  "active_users": 38,
  "timestamp": 1714000060000
}
```

Key concepts: **Redis Pub/Sub**, **WebSocket scaling** (sticky sessions or Redis adapter), **fan-out**

---

#### 8. `project-service`

**Job:** CRUD for projects, API keys, event schema definitions, team members.

- Issues API keys (hashed, stored in DB)
- Defines allowed events and their property schemas per project
- Consumed by validation-service via Redis cache

---

#### 9. `auth-service`

Standard JWT auth for dashboard users. Separate from API key auth (which lives in the gateway).

---

### Data Stores Per Service

| Service          | Database                 | Why                                       |
| ---------------- | ------------------------ | ----------------------------------------- |
| ingest           | None (stateless)         | —                                         |
| validation       | Redis (schema cache)     | Fast lookup, invalidated on schema change |
| enrichment       | Redis (sessions)         | TTL-based session windows                 |
| stream-processor | Redis + TimescaleDB      | Live counters + time-series               |
| raw-storage      | ClickHouse               | Columnar OLAP                             |
| query-api        | TimescaleDB + ClickHouse | Reads only                                |
| project/auth     | PostgreSQL               | Relational, transactional                 |

---

### Kafka Topics

```
raw-events
validated-events
dead-letter-events
enriched-events
metric-updates          ← stream-processor → Redis Pub/Sub bridge
```

---

### SDK (bonus — very satisfying to build)

```typescript
// What your users embed
import { Analytics } from "@yourplatform/sdk";

const analytics = new Analytics({ apiKey: "pk_live_abc123" });

analytics.track("page_view", {
  url: window.location.href,
  referrer: document.referrer,
});

analytics.identify("usr_123", {
  email: "seif@example.com",
  plan: "pro",
});
```

The SDK handles: batching, retry with backoff, anonymous ID generation, localStorage persistence, and flushing on `beforeunload`.

---

### Stack

```
Language:      TypeScript / Bun
Message broker: Kafka (Redpanda locally — Kafka-compatible, much lighter)
Cache:         Redis
Time-series:   TimescaleDB (Postgres extension, familiar)
OLAP:          ClickHouse
Gateway:       Custom Express or Traefik
Containers:    Docker Compose
Tracing:       OpenTelemetry + Jaeger
Metrics:       Prometheus + Grafana
```

> **Redpanda** instead of Kafka locally — same API, no JVM, starts in seconds. Switch to real Kafka for prod.

---

This project is uniquely good because **every microservice concept appears naturally** — you're not forcing patterns in. The event pipeline literally is the product.
