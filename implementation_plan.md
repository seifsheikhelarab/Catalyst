## Implementation Plan

---

### Phase 0 — Foundation (Week 1)

**Goal:** Monorepo, shared tooling, local infrastructure up.

**Tasks:**

- Init monorepo with Turborepo or `pnpm workspaces`
- Setup shared packages: `@app/logger`, `@app/kafka`, `@app/redis`, `@app/types`
- Docker Compose with: Redpanda, Redis, PostgreSQL, TimescaleDB, ClickHouse
- Redpanda Console UI for topic inspection
- Shared Zod schemas for all Kafka message types
- ESLint + Prettier + `tsconfig` base

**Deliverable:** `docker compose up` starts all infra. Shared packages importable across services.

---

### Phase 1 — Ingest Pipeline (Week 2)

**Goal:** Events flow from HTTP → Kafka → validated → enriched.

**Services built:** `ingest-service`, `validation-service`, `enrichment-service`

**Tasks:**

`ingest-service`

- Express app, `POST /track` endpoint
- Validate only `event` + `projectId` present
- Publish to `raw-events` Kafka topic
- Return `202` immediately
- Idempotency: hash `(projectId + userId + timestamp + event)` → dedup key in Redis with 60s TTL

`validation-service`

- Kafka consumer on `raw-events`
- Full Zod schema validation
- Valid → `validated-events`, invalid → `dead-letter-events`
- Log rejection reasons

`enrichment-service`

- Kafka consumer on `validated-events`
- MaxMind GeoLite2 for IP → geo (local DB file, no external call)
- `ua-parser-js` for User-Agent parsing
- Session stitching: Redis key `session:{projectId}:{userId}` with 30min TTL
- Publish to `enriched-events`

**Deliverable:** Send a `POST /track` → see enriched event in `enriched-events` topic via Redpanda Console.

---

### Phase 2 — Storage Layer (Week 3)

**Goal:** Enriched events land in both ClickHouse and TimescaleDB.

**Services built:** `raw-storage-service`, initial `stream-processor-service`

**Tasks:**

`raw-storage-service`

- Kafka consumer on `enriched-events`
- Buffer events in memory (1000 events or 5s flush interval)
- Batch insert into ClickHouse `events` table
- ClickHouse schema:

```sql
CREATE TABLE events (
  project_id    String,
  event         String,
  user_id       String,
  session_id    String,
  country       String,
  city          String,
  device_type   String,
  browser       String,
  os            String,
  properties    String,  -- JSON
  timestamp     DateTime64(3)
) ENGINE = MergeTree()
PARTITION BY toYYYYMM(timestamp)
ORDER BY (project_id, event, timestamp);
```

`stream-processor-service` (basic)

- Kafka consumer on `enriched-events`
- Increment Redis counters:
  - `counter:{projectId}:{event}:{bucket_1min}`
  - `counter:{projectId}:{event}:{bucket_1hr}`
  - `counter:{projectId}:{event}:{bucket_1day}`
- HyperLogLog for unique users: `hll:{projectId}:{date}`
- Write hourly rollups to TimescaleDB hypertable

TimescaleDB schema:

```sql
CREATE TABLE event_rollups (
  project_id  TEXT,
  event       TEXT,
  bucket      TIMESTAMPTZ,
  count       BIGINT,
  unique_users BIGINT
);
SELECT create_hypertable('event_rollups', 'bucket');
```

**Deliverable:** Send 100 test events → query ClickHouse and see raw rows. Query TimescaleDB and see hourly rollups.

---

### Phase 3 — Project & Auth Services (Week 4)

**Goal:** Real projects, API keys, authenticated dashboard users.

**Services built:** `project-service`, `auth-service`, `api-gateway`

**Tasks:**

`project-service`

- CRUD: create project, get project, list projects
- API key generation: `pk_live_{random32}` → hash with bcrypt → store hash
- Event schema definitions per project (stored in PostgreSQL, cached in Redis)
- Expose internal endpoint for validation-service to fetch schemas

`auth-service`

- Register / login for dashboard users
- JWT access token (15min) + refresh token (7d)
- Org/workspace scoping (familiar from CRM)

`api-gateway`

- Two auth strategies:
  - **API key** (ingest path): extract `Authorization: Bearer pk_live_...`, validate against project-service
  - **JWT** (dashboard path): validate and forward user context
- Rate limiting per API key (Redis sliding window)
- Route table:
  - `POST /track` → ingest-service
  - `GET /api/*` → query-api-service
  - `WS /live` → websocket-service

**Deliverable:** Full authenticated flow. Create project → get API key → use it to POST /track → rejected without valid key.

---

### Phase 4 — Query API (Week 5)

**Goal:** Dashboard can fetch historical metrics.

**Services built:** `query-api-service`

**Tasks:**

Endpoints:

```
GET /projects/:id/metrics
  ?event=page_view
  &from=2024-01-01
  &to=2024-01-31
  &granularity=day

GET /projects/:id/funnels
  ?steps=["signup","onboard","purchase"]
  &window=7d
  &from=...&to=...

GET /projects/:id/retention
  ?cohortEvent=signup
  &returnEvent=login
  &periods=8

GET /projects/:id/users
  ?filter=country:EG
  &limit=50&offset=0

GET /projects/:id/events/live
  (last 60 seconds from Redis counters)
```

Implementation:

- Simple time-range queries → TimescaleDB (fast, pre-aggregated)
- Funnel + retention + user-level queries → ClickHouse
- Cache frequent queries: Redis with `query:{hash}` key, 30s TTL
- Cache invalidation: TTL only (acceptable for analytics, not finance)

**Deliverable:** Postman collection hitting all endpoints with real data returned.

---

### Phase 5 — Live Dashboard (Week 6)

**Goal:** Real-time metrics pushed to connected clients.

**Services built:** `websocket-service`

**Tasks:**

- `ws-service` subscribes to Redis Pub/Sub channel `live:{projectId}`
- `stream-processor` publishes to that channel on every 1-min bucket close
- Client connects: `ws://gateway/live?projectId=proj_abc&token=...`
- Gateway authenticates then proxies WebSocket to ws-service
- ws-service rooms by `projectId`, fans out updates to all connected dashboards
- Payload:

```json
{
  "type": "metric_update",
  "projectId": "proj_abc",
  "event": "page_view",
  "count_1m": 142,
  "active_users": 38,
  "timestamp": 1714000060000
}
```

**Deliverable:** Open two browser tabs on dashboard → both receive live updates simultaneously as you send test events.

---

### Phase 6 — SDK (Week 7)

**Goal:** Embeddable client SDK.

**Package:** `@app/sdk` (published or imported locally)

**Tasks:**

- Auto-generate anonymous `deviceId` (UUID stored in localStorage)
- Batch events: flush every 5s or when batch hits 20 events
- Retry with exponential backoff on network failure (3 attempts)
- Flush on `beforeunload`
- Methods: `track(event, properties)`, `identify(userId, traits)`, `page()`
- Source maps + minified build via `tsup`

```typescript
const analytics = new Analytics({ apiKey: "pk_live_abc" });
analytics.identify("usr_123", { plan: "pro" });
analytics.track("button_click", { buttonId: "upgrade-cta" });
analytics.page();
```

**Deliverable:** Drop SDK into any HTML page → events appear in your dashboard live.

---

### Phase 7 — Observability (Week 8)

**Goal:** Full visibility across all services.

**Tasks:**

Distributed tracing

- Add OpenTelemetry SDK to every service
- Propagate `traceparent` header through Kafka messages (custom header)
- Deploy Jaeger → trace one event from `POST /track` all the way to WebSocket push

Metrics

- Expose `/metrics` (Prometheus format) from every service
- Key metrics per service:
  - ingest: `events_received_total`, `kafka_publish_latency_ms`
  - validation: `events_valid_total`, `events_rejected_total`
  - stream-processor: `processing_lag_ms`, `buckets_flushed_total`
  - query-api: `query_duration_ms`, `cache_hit_ratio`
- Deploy Prometheus + Grafana
- Build Kafka consumer lag dashboard (most important operational metric)

Logging

- `@app/logger` wraps Winston, outputs structured JSON
- Every log includes: `service`, `traceId`, `projectId`, `level`
- Deploy Loki + Grafana Logs (or just ELK if preferred)

**Deliverable:** One Grafana dashboard showing end-to-end pipeline health. One Jaeger trace showing full event journey.

---

### Phase 8 — Resilience (Week 9)

**Goal:** System degrades gracefully, doesn't collapse.

**Tasks:**

Circuit breakers

- Wrap ClickHouse calls in `opossum` circuit breaker
- On open circuit → serve from TimescaleDB cache or return stale Redis data
- Alert via Prometheus when circuit opens

Dead letter queue processor

- Consume `dead-letter-events`
- Log with full context, store in PostgreSQL for manual inspection
- Admin endpoint: `GET /admin/dlq` to view, `POST /admin/dlq/:id/retry` to requeue

Kafka consumer resilience

- Implement manual offset commit (don't auto-commit)
- On processing failure: retry 3x → send to DLQ → commit offset
- Prevents poison pill messages from blocking the consumer

Graceful shutdown

- On `SIGTERM`: stop accepting new work, drain in-flight processing, flush buffers, disconnect cleanly
- Kubernetes `preStop` hook gives 30s grace period

**Deliverable:** Kill ClickHouse mid-load → query-api serves degraded but alive. Restart it → circuit closes automatically.

---

### Phase 9 — Kubernetes (Week 10)

**Goal:** Production-grade deployment.

**Tasks:**

- Write `Deployment` + `Service` + `ConfigMap` + `Secret` manifests per service
- Horizontal Pod Autoscaler on `ingest-service` (CPU-based) and `stream-processor` (Kafka lag-based — KEDA)
- Redpanda Helm chart
- Redis Sentinel or Redis Cluster
- Ingress (Nginx) replacing local gateway routing
- Resource limits + liveness/readiness probes on every pod
- Sealed Secrets or external-secrets for credentials

**Deliverable:** Full system running on a local k3s cluster or a cheap VPS. `kubectl get pods` shows everything healthy.

---

### Summary Timeline

| Phase | Focus              | Week |
| ----- | ------------------ | ---- |
| 0     | Foundation & infra | 1    |
| 1     | Ingest pipeline    | 2    |
| 2     | Storage layer      | 3    |
| 3     | Project & auth     | 4    |
| 4     | Query API          | 5    |
| 5     | Live WebSocket     | 6    |
| 6     | Client SDK         | 7    |
| 7     | Observability      | 8    |
| 8     | Resilience         | 9    |
| 9     | Kubernetes         | 10   |

---

Each phase has a concrete deliverable so you always know when it's done. Phases 0–5 give you a working product. Phases 6–9 make it production-grade.
