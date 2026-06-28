# Catalyst — Real-Time Analytics Pipeline

> **Product analytics platform** — embed a tiny SDK, track millions of events in real-time, query live dashboards with rolling windows, funnels, and retention cohorts.

---

## Architecture

- **Microservices Architecture** — 10 independent services communicating via Kafka message broker, each horizontally scalable
- **Event-Driven Pipeline** — 4-stage processing chain: Ingest → Validate → Enrich → Store/Stream
- **CQRS Pattern** — Separate write path (Kafka consumers) from read path (Query API + WebSocket push)
- **Bun Runtime** — All services built with TypeScript on Bun (fast cold starts, native TypeScript, built-in test runner, WebSocket server)
- **Monorepo** — Bun workspaces with 7 shared packages, path aliases, oxlint/oxfmt for code quality

---

## Services

### 1. API Gateway (Port 3000)

- **Unified Entry Point** — Routes all external traffic to backend services
- **Dual Authentication** — API key (`pk_live_*` with SHA-256 fingerprint) for event ingest, JWT for dashboard/management
- **API Key Validation** — Delegates to project-service for bcrypt verification
- **Sliding Window Rate Limiting** — Per API key via Redis (configurable max requests per 60s window)
- **WebSocket Proxy** — Upgrades HTTP to WS, validates JWT, proxies to websocket-service with bidirectional forwarding
- **Request Routing** — `POST /track` → ingest-service, `/auth/*` → auth-service, `/api/*` → query-api-service, `/projects/*` → project-service, `/admin/*` → dlq-processor-service
- **OpenAPI Documentation** — Auto-generated interactive API docs at `/docs` via Scalar (includes WebSocket endpoint docs)
- **Health & Metrics** — `/health` endpoint, `/metrics` endpoint with Prometheus counters

### 2. Ingest Service (Port 3004)

- **Event Reception** — `POST /track` with minimal Zod validation (event + projectId + timestamp required)
- **Idempotency Deduplication** — SHA-256 hash of (projectId + userId + timestamp + event) → Redis dedup key with 60s TTL, returns `202 Accepted` with `status: "duplicate"` for repeats
- **Kafka Publishing** — Publishes to `raw-events` topic with OpenTelemetry trace headers and injected traceId
- **Prometheus Metrics** — Events received counter, duplicate events counter, Kafka publish latency histogram
- **Distributed Tracing** — Creates spans for each publish, propagates W3C trace context

### 3. Validation Service

- **Schema Validation** — Kafka consumer on `raw-events`, validates against Zod `RawEventSchema`
- **Dead Letter Queue** — Invalid events published to `dead-letter-events` with full error details
- **Retry with Backoff** — Kafka produce failures retried 3x with exponential backoff (200ms, 400ms, 800ms) before DLQ
- **Manual Offset Management** — Uses `autoCommit: false` with `eachBatch`, manual `resolveOffset`, heartbeat tracking
- **Graceful Shutdown** — 25s deadline to drain in-flight messages, disconnect consumer/producer
- **Distributed Tracing** — Extracts traceparent from Kafka headers, creates child spans
- **Metrics Server** — Dedicated metrics port (9101) with valid/rejected/Kafka-failed counters, processing duration histogram

### 4. Enrichment Service

- **GeoIP Enrichment** — MaxMind GeoLite2 local database lookup (IP → country, city) — no external API calls
- **User-Agent Parsing** — ua-parser-js extracts browser, OS, device type from user agent string
- **Session Stitching** — Redis-based session tracking: 30-min inactivity TTL, auto-creates new UUID session IDs
- **Identity Resolution** — Sessions tied to userId when available
- **Retry with Backoff** — Kafka produce failures retried 3x before DLQ
- **Dead Letter Queue** — Unrecoverable errors sent to DLQ with full context
- **Manual Offset Management** — `eachBatch` with manual commit, heartbeat, staleness detection
- **Graceful Shutdown** — Drain in-flight, disconnect consumer/producer/redis, 25s deadline
- **Metrics Server** — Dedicated metrics port (9102) with processed/retried/DLQ counters, processing duration histogram

### 5. Raw Storage Service

- **Micro-Batching** — In-memory buffer flushed to ClickHouse at 1000 events or 5s interval (whichever first)
- **ClickHouse Storage** — MergeTree table partitioned by month, ordered by (project_id, event, timestamp), 90-day TTL
- **Bulk Inserts** — Batched INSERT with formatted values for maximum throughput
- **Circuit Breaker** — opossum-based breaker around ClickHouse inserts (3 failures opens circuit for 15s, auto-recovery)
- **Backpressure Protection** — Max buffer size of 50,000 events; drops oldest events when exceeded
- **Prometheus Gauges** — Circuit state (0=closed, 1=half-open, 2=open), buffer size reported every second
- **Insert Duration Histogram** — Track ClickHouse insert latency (50ms–5s buckets)
- **Graceful Shutdown** — Flushes remaining buffer on shutdown, clears timer, 25s deadline
- **Metrics Server** — Dedicated metrics port (9103)

### 6. Stream Processor Service

- **Real-Time Counters** — Redis INCR for 3 time buckets per project/event: 1-minute, 1-hour, 1-day
- **HyperLogLog Uniques** — Redis PFADD for approximate unique user counts per project/event/day (7-day TTL, space-efficient)
- **Hourly Rollups** — Every 60s, scans deferred Redis counter keys, aggregates into TimescaleDB hypertable with upsert (ON CONFLICT DO UPDATE)
- **Live Update Publishing** — On each rollup, publishes JSON to Redis Pub/Sub `live:{projectId}` channel for WebSocket fan-out
- **Retry with Backoff** — Event processing failures retried 3x before DLQ
- **Dead Letter Queue** — Unrecoverable errors sent to DLQ with full context
- **Manual Offset Management** — `eachBatch` with manual commit, heartbeat tracking
- **Graceful Shutdown** — Final rollup flush before disconnect, 25s deadline
- **Metrics Server** — Dedicated metrics port (9104) with processed/retried/DLQ counters, rollup flushes counter, processing lag histogram

### 7. Query API Service (Port 3003)

- **Metrics Query** — `GET /projects/:id/metrics` — Time-bucketed event counts from TimescaleDB with minute/hour/day granularity
- **Funnel Analysis** — `GET /projects/:id/funnels` — Step-to-step conversion query with configurable time window (ClickHouse JOIN-based)
- **Retention Analysis** — `GET /projects/:id/retention` — Weekly cohort retention with CTEs (ClickHouse)
- **User Query** — `GET /projects/:id/users` — Paginated distinct user list with field-level filtering and first/last seen
- **Live Counters** — `GET /projects/:id/events/live` — Current-minute event counts from Redis counters via SCAN
- **Query Caching** — Redis cache with SHA-1 query hash keys, 30-second TTL, tracked as cache hit ratio metric
- **Circuit Breaker** — ClickHouse queries wrapped in opossum circuit breaker (returns 503 with error when open)
- **Project Authorization** — Validates project ownership via X-Org-Id header against project-service with Redis caching (5-min TTL)
- **SQL Injection Protection** — Identifier whitelist regex, proper parameterized queries for TimescaleDB
- **Prometheus Metrics** — Query counts, duration histogram, cache hits, circuit fallback counts, circuit state gauge

### 8. Project Service (Port 3001)

- **Project CRUD** — Create, list, and retrieve projects with PostgreSQL persistence
- **API Key Management** — Generate `pk_live_` prefixed keys with bcrypt hashing, SHA-256 prefix for indexed lookup, optional expiration
- **API Key Validation** — Internal endpoint `POST /internal/keys/:key/validate` for gateway (compares bcrypt hash)
- **Event Schema Registry** — Create/update versioned event schemas per project (JSONB), automatic version increment on updates
- **Schema Caching** — Retrieved schemas cached in Redis with 300s TTL, invalidated on updates
- **Prometheus Metrics** — Request counter

### 9. Auth Service (Port 3002)

- **User Registration** — Creates org + user in single PostgreSQL transaction with bcrypt password hashing (10 rounds)
- **Login** — Email/password verification with bcrypt, returns access + refresh tokens
- **JWT Tokens** — HS256-signed via jose: access tokens (15-min TTL), refresh tokens (7-day TTL)
- **Token Refresh** — Verify refresh token, check Redis for revocation, issue new access token
- **Token Revocation** — Refresh tokens stored in Redis with TTL; logout deletes them; refresh endpoint checks existence
- **User Profile** — `GET /auth/me` returns current user's id, org_id, email
- **Duplicate Email Protection** — Unique constraint catches existing emails during registration (409 Conflict)
- **Prometheus Metrics** — Request counter, login success/failure counters

### 10. WebSocket Service (Port 3005)

- **Real-Time Connections** — WebSocket server for live event metric pushes
- **Room-Based Broadcasting** — Clients grouped by projectId, messages fanned out to all connected clients in the room
- **Redis Pub/Sub Integration** — Subscribes to `live:*` channels, bridges Redis messages to WebSocket clients
- **JWT Verification** — Validates token on connection, closes with 4001 on invalid tokens
- **Connection Tracking** — Room size logging, connection/disconnection lifecycle
- **Prometheus Metrics** — Connection counter
- **Health Check** — `/health` and `/metrics` endpoints

### 11. DLQ Processor Service (Port 3006)

- **DLQ Consumption** — Kafka consumer on `dead-letter-events`, persists envelopes to PostgreSQL `dlq_events` table
- **Admin Dashboard API** — Full CRUD for DLQ events:
  - `GET /admin/dlq` — Paginated list with status filter (pending/retrying/retried/discarded)
  - `GET /admin/dlq/:id` — Full event details including original value and headers
  - `POST /admin/dlq/:id/retry` — Re-publishes original message to original topic with optimistic locking (FOR UPDATE)
  - `POST /admin/dlq/:id/discard` — Marks event as discarded
- **Manual Offset Management** — `eachBatch` with manual commit
- **Graceful Shutdown** — 25s deadline to drain in-flight messages
- **Prometheus Metrics** — Stored events counter, retry requests/failures counters
- **Retry Tracking** — Updates retry count and last error on failure

---

## Shared Packages

### @catalyst/types
- **Zod Schemas** — Type-safe schemas for all Kafka message types: RawEvent, ValidatedEvent, EnrichedEvent, DeadLetterEvent, DLQEnvelope
- **Domain Schemas** — Project, User, ApiKey, EventSchemaDefinition with Zod validation
- **Reusable Types** — Inferred TypeScript types exported alongside schemas

### @catalyst/kafka
- **Kafka Client Singleton** — Lazily initialized Kafka instance with configurable brokers, SSL, SASL
- **Producer Management** — Singleton producer with LegacyPartitioner, lazy connect
- **Consumer Factory** — Configurable consumer with session timeout and heartbeat interval
- **Retry & DLQ Utilities** — `processWithRetryAndDLQ()` implements retry-then-DLQ pattern; `sendToDLQ()` serializes full message context including headers
- **Topic Constants** — Centralized topic names: raw-events, validated-events, enriched-events, dead-letter-events
- **Header Serialization** — Converts Kafka message headers to plain objects and back

### @catalyst/redis
- **Redis Client Singleton** — Singleton Redis (ioredis) with lazy connect, configurable host/port/password/db
- **Connection Management** — Explicit connect/disconnect, error logging, max retry config

### @catalyst/logger
- **Structured Logging** — Pino-based logger with JSON output in production, pretty-printing in development
- **Global Log Flush** — `flushLogs()` drains all logger instances (essential for graceful shutdown)
- **Configurable** — Service name, log level, pretty-print toggle

### @catalyst/metrics
- **Prometheus Registry** — Shared registry across all services
- **Metric Factories** — Counter, Gauge, Histogram creation with auto-registration
- **Metrics Handler** — `metricsHandler()` returns Prometheus-formatted response for `/metrics` endpoints

### @catalyst/tracing
- **OpenTelemetry Setup** — NodeTracerProvider with BatchSpanProcessor (efficient async export)
- **OTLP Export** — Sends traces to Jaeger via OTLP/protobuf
- **W3C Trace Context** — Full `traceparent` propagation via `injectTraceHeaders()` and `extractTraceFromHeaders()`
- **Kafka Trace Propagation** — `startSpanWithTraceContext()` extracts traceparent from Kafka message headers, creates child spans
- **HTTP Instrumentation** — Auto-instruments all HTTP calls for trace visibility
- **Graceful Shutdown** — `shutdownTracing()` flushes and shuts down provider

### @catalyst/circuit-breaker
- **Opossum Wrapper** — `createBreaker()` with configurable timeout, error threshold, reset timeout, volume threshold
- **Fallback Support** — Optional fallback function called when circuit is open
- **State Tracking** — TrackedBreaker class with `isOpen`, `state`, `stats` getters
- **Event Logging** — Logs circuit open/half-open/close state changes
- **Metrics Friendly** — Gauges can poll `isOpen` for Prometheus reporting

### @catalyst/sdk (Browser SDK)
- **Analytics Class** — Embeddable SDK with simple API: `track()`, `identify()`, `page()`
- **Anonymous Device ID** — Auto-generated UUID persisted in localStorage
- **Event Batching** — Buffers events and flushes every 5 seconds or when batch reaches 20 events
- **Exponential Backoff Retry** — 3 retry attempts with backoff (200ms, 400ms, 800ms)
- **Page View Tracking** — Auto-captures URL, referrer, document title via `page()`
- **Graceful Unload** — Flushes pending events via `navigator.sendBeacon` on `beforeunload`
- **Polyfilled UUID** — Fallback for browsers without `crypto.randomUUID()`

---

## Observability

### Distributed Tracing (Jaeger)
- **End-to-End Trace Propagation** — W3C traceparent propagated through Kafka headers across all 5 pipeline stages: Ingest → Validation → Enrichment → Raw-Storage / Stream-Processor
- **BatchSpanProcessor** — Efficient async span export (batches spans before sending)
- **OTLP Protocol** — Standard OpenTelemetry protocol to Jaeger
- **HTTP Auto-Instrumentation** — All HTTP calls automatically captured in traces

### Metrics (Prometheus + Grafana)
- **Per-Service Metrics** — Every service exposes a `/metrics` endpoint
- **Service-Specific Metrics**:
  - Gateway: request count, WebSocket connection count
  - Ingest: events received, deduped, Kafka publish latency (histogram)
  - Validation: valid/rejected/Kafka-failed counts, processing duration (histogram)
  - Enrichment: processed/retried/DLQ counts, processing duration (histogram)
  - Raw Storage: batches inserted, events stored, events dropped, circuit state (gauge), buffer size (gauge), insert duration (histogram)
  - Stream Processor: events processed, retried/DLQ, rollups flushed, processing lag (histogram)
  - Query API: requests, duration (histogram), cache hits, circuit fallbacks, circuit state (gauge)
  - WebSocket: connections
  - Auth: requests, login success/failure
  - Project: requests
  - DLQ: events stored, retry requests/failures
- **Grafana Dashboard** — 12-panel pipeline overview with timeseries for ingest rate, validation rate, enrichment rate, ClickHouse storage rate, stream processing rate, processing lag P99, Kafka publish latency P99, insert duration P99, query API rate & cache hit ratio, query duration P99, WebSocket connections, gateway rate
- **Prometheus Scrape Config** — Scrapes all 10 service ports + Redpanda metrics
- **Provisioned Datasources** — Prometheus, Loki, Jaeger auto-configured in Grafana

### Logging (Pino + Loki)
- **Structured JSON** — All logs include service name, traceId, level, projectId
- **Pretty-Printing** — Colorized output in development, JSON in production
- **Global Flush** — `flushLogs()` drains all loggers before shutdown
- **Loki Integration** — Grafana Loki configured for log aggregation

### Infrastructure Monitoring
- **Docker Health Checks** — All containers (Redpanda, Redis, PostgreSQL, ClickHouse, Jaeger, Prometheus, Grafana, Loki) have health checks with retries and start periods
- **Redpanda Admin API** — Exposes cluster health on port 9644

---

## Infrastructure & DevOps

### Docker Compose (11 Containers)
- **Redpanda** — Kafka-compatible message broker (no JVM, fast startup) with console UI
- **Redis** — In-memory cache with AOF persistence
- **TimescaleDB** — PostgreSQL with TimescaleDB extension for time-series hypertables
- **ClickHouse** — Columnar OLAP database for raw event storage
- **Jaeger** — Distributed tracing with OTLP ingestion
- **Prometheus** — Metrics collection with 15s scrape interval
- **Grafana** — Dashboard visualization with provisioned datasources and dashboards
- **Loki** — Log aggregation with TSDB storage
- **Redpanda Console** — Web UI for inspecting Kafka topics, consumer groups, and messages
- **Named Volumes** — Persistent storage for all databases and observability tools
- **Custom Network** — `catalyst-net` for inter-service communication
- **Resource Limits** — Memory limits, ulimit configs for ClickHouse

### Database Schema Initialization (4 SQL Scripts)
- **init-clickhouse.sql** — Creates `events` MergeTree table with 90-day TTL, partitioned by month
- **init-timescaledb.sql** — Creates `event_rollups` hypertable with indexes on project and event
- **init-phase3.sql** — Creates `orgs`, `users`, `projects`, `api_keys`, `event_schemas` tables with constraints, indexes, and cascading deletes
- **init-phase8.sql** — Creates `dlq_events` table with status tracking, retry counting, and performance indexes

### Container Management
- **Health Check Chains** — Grafana waits for Prometheus+Loki, Prometheus waits for Jaeger, services wait for Kafka/Redis/DB
- **Lifecycle Management** — `docker compose up` starts all infrastructure with proper dependency ordering

---

## Resilience & Production Readiness

- **Retry-then-DLQ Pattern** — 3 services (validation, enrichment, stream-processor) implement exponential backoff with DLQ fallback
- **Circuit Breakers** — 2 services (raw-storage, query-api) wrap ClickHouse calls with opossum circuit breakers
- **Graceful Shutdown** — All services handle SIGTERM/SIGINT: stop consuming, drain in-flight with 25s deadline, flush buffers, disconnect cleanly
- **Buffer Overflow Protection** — Raw-storage service has 50,000 event max buffer; drops oldest under backpressure
- **Idempotency** — Ingest-service deduplicates events via Redis SHA-256 key with 60s TTL
- **Rate Limiting** — API Gateway implements per-key sliding window rate limiting via Redis
- **Manual Kafka Offset Commits** — All consumers use `autoCommit: false` with explicit `resolveOffset` and `commitOffsetsIfNecessary`
- **Consumer Heartbeat Tracking** — Heartbeat called after each message, `isRunning()`/`isStale()` checks prevent processing during rebalance
- **Poison Pill Prevention** — Failed messages sent to DLQ rather than blocking consumption
- **SQL Injection Protection** — Identifier whitelist regex validation in query-api-service
- **Optimistic Locking** — DLQ retry uses `SELECT ... FOR UPDATE` to prevent concurrent processing

---

## Data Pipeline

### Kafka Topics
```
raw-events → validated-events → enriched-events → (stream-processor / raw-storage)
                                          ↳ dead-letter-events
```

### Pipeline Stages
1. **Ingest** — HTTP → minimal validation → dedup → Kafka (raw-events)
2. **Validate** — Zod schema check → valid → Kafka (validated-events), invalid → DLQ
3. **Enrich** — GeoIP lookup + UA parsing + session stitching → Kafka (enriched-events)
4. **Store** — ClickHouse batch insert (raw events for ad-hoc querying)
5. **Stream** — Redis counters + HyperLogLog + TimescaleDB rollups + live Pub/Sub

### Data Flow
```
Client SDK → API Gateway → Ingest → [Kafka] → Validate → [Kafka] → Enrich → [Kafka]
                                                                           ↓
                                                                   ┌───────┴───────┐
                                                                   ↓               ↓
                                                             Stream Proc    Raw Storage
                                                             (Redis+TSDB)   (ClickHouse)
                                                                   ↓
                                                             Query API ← WebSocket
                                                                   ↓
                                                             Dashboard UI
```

---

## Tech Stack

| Category            | Technology                                  |
| ------------------- | ------------------------------------------- |
| Runtime             | TypeScript / Bun                            |
| Message Broker      | Kafka (Redpanda locally)                    |
| Cache               | Redis (ioredis)                             |
| Time-Series DB      | TimescaleDB (PostgreSQL extension)          |
| OLAP                | ClickHouse                                  |
| Relational DB       | PostgreSQL                                  |
| HTTP Framework      | Hono                                        |
| Auth                | jose (JWT), bcrypt                          |
| Validation          | Zod                                         |
| Tracing             | OpenTelemetry + Jaeger                      |
| Metrics             | Prometheus client + Grafana                 |
| Logging             | Pino + Loki                                 |
| Circuit Breaker     | opossum                                     |
| GeoIP               | MaxMind GeoLite2 (geoip-lite)               |
| UA Parsing          | ua-parser-js                                |
| API Documentation   | Scalar (OpenAPI 3.0)                        |
| Package Manager     | Bun workspaces (monorepo)                   |
| Linting/Formatting  | oxlint / oxfmt                              |
| Containerization    | Docker Compose                              |

---

## Service Ports

| Service               | HTTP Port | Metrics Port |
| --------------------- | --------- | ------------ |
| API Gateway           | 3000      | —            |
| Project Service       | 3001      | —            |
| Auth Service          | 3002      | —            |
| Query API Service     | 3003      | —            |
| Ingest Service        | 3004      | —            |
| WebSocket Service     | 3005      | —            |
| DLQ Processor Service | 3006      | —            |
| Validation Service    | —         | 9101         |
| Enrichment Service    | —         | 9102         |
| Raw Storage Service   | —         | 9103         |
| Stream Processor      | —         | 9104         |

---

## Development Tooling

- **Single-Command Startup** — `bun run start` launches all 11 services via `scripts/start-all.ts`
- **Individual Service Runs** — Each service can be started independently with `bun run <service>`
- **Linting** — `oxlint` for code quality and style enforcement
- **Formatting** — `oxfmt` for consistent code formatting
- **Type Checking** — Full TypeScript strict mode across all packages
- **GeoLite2 Setup** — `scripts/download-geolite2.sh` downloads MaxMind GeoLite2-City database
