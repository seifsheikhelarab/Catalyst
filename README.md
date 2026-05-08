# Catalyst - Real-Time Analytics Pipeline

A high-performance product analytics platform built with a streaming-first architecture. Catalyst ingests millions of events, processes them in real-time, and serves live dashboards with rolling windows, funnels, and retention metrics.

## Architecture

```
Client SDK → API Gateway → Ingest Service → Kafka (raw-events)
                                    ↓
                              Validation Service → Kafka (validated-events)
                                    ↓
                              Enrichment Service → Kafka (enriched-events)
                                    ↓
                        ┌──────────────┴──────────────┐
                        ↓                             ↓
              Stream Processor              Raw Storage Service
              (Redis + TimescaleDB)          (ClickHouse)
                        ↓
              Query API Service → Dashboard
              WebSocket Service → Live Updates
```

## Stack

| Component | Technology |
|-----------|------------|
| Runtime | TypeScript / Bun |
| Message Broker | Kafka (Redpanda locally) |
| Cache | Redis |
| Time-series | TimescaleDB (PostgreSQL extension) |
| OLAP | ClickHouse |
| Relational | PostgreSQL |

## Services

| Service | Purpose |
|---------|---------|
| `ingest-service` | Receive events, publish to Kafka, return 202 |
| `validation-service` | Zod schema validation per project |
| `enrichment-service` | GeoIP, UA parsing, session stitching |
| `stream-processor-service` | Rolling windows, funnels, retention, HyperLogLog |
| `raw-storage-service` | Batch write to ClickHouse |
| `query-api-service` | REST API for dashboards |
| `websocket-service` | Push live metric updates |
| `project-service` | CRUD for projects, API keys |
| `auth-service` | JWT auth for dashboard |
| `api-gateway` | Auth, rate limiting, routing |

## Quick Start

### Prerequisites

- [Bun](https://bun.sh) (runtime)
- [Docker](https://docker.com) + Docker Compose
- [Redpanda Console](https://docs.redpanda.com/current/get-started/quick-start/) (optional, for topic inspection)

### 1. Start Infrastructure

```bash
docker compose up -d
```

This starts: Redpanda, Redis, PostgreSQL, TimescaleDB, ClickHouse, Redpanda Console.

### 2. Install Dependencies

```bash
bun install
```

### 3. Run Services

Each service can be run independently:

```bash
# Terminal 1: Ingest service
bun --filter ingest-service dev

# Terminal 2: Validation service  
bun --filter validation-service dev

# Terminal 3: Enrichment service
bun --filter enrichment-service dev
```

### 4. Test the Pipeline

```bash
curl -X POST http://localhost:3000/track \
  -H "Content-Type: application/json" \
  -d '{
    "event": "page_view",
    "projectId": "proj_abc",
    "userId": "usr_123",
    "properties": {
      "url": "/pricing",
      "referrer": "google.com"
    },
    "timestamp": 1714000000000
  }'
```

Expected response: `202 Accepted`

### 5. Inspect Events

Visit [Redpanda Console](http://localhost:6789) to see events flowing through topics:
- `raw-events`
- `validated-events`
- `enriched-events`

## Kafka Topics

```
raw-events → validated-events → enriched-events → (stream-processor / raw-storage)
                                          ↳ dead-letter-events
```

## Key Concepts

- **Back-pressure handling** in ingest-service (fire-and-forget)
- **Idempotency keys** in Redis with 60s TTL for dedup
- **Consumer groups** with manual offset commit
- **Circuit breakers** around ClickHouse calls
- **Graceful shutdown**: drain in-flight, flush buffers, then exit
- **Distributed tracing**: OpenTelemetry with traceparent propagated through Kafka headers

## Project Phases

See `implementation_plan.md` for the full development timeline:

- **Phase 0** — Foundation (Week 1): Monorepo, shared tooling, Docker Compose
- **Phase 1** — Ingest Pipeline (Week 2): Events flow from HTTP → Kafka → validated → enriched
- **Phase 2** — Storage Layer (Week 3): ClickHouse and TimescaleDB integration
- **Phase 3** — Stream Processing (Week 4): Real-time aggregates in Redis
- **Phase 4** — Query API (Week 5): REST API for dashboards
- **Phase 5** — WebSocket Service (Week 6): Live updates
- **Phase 6** — Project Service (Week 7): Project CRUD, API keys
- **Phase 7** — Auth Service (Week 8): JWT authentication
- **Phase 8** — Polish (Week 9): Circuit breakers, rate limiting, error handling
- **Phase 9** — Ship (Week 10): Docker Compose production-ready

## API Reference

### Ingest Service

```bash
POST /track
{
  "event": "page_view",
  "projectId": "proj_abc",
  "userId": "usr_123",
  "properties": {
    "url": "/pricing"
  },
  "timestamp": 1714000000000
}
```

Response: `202 Accepted`

### Query API

```bash
GET /projects/:id/metrics?event=page_view&from=2024-01-01&to=2024-01-31&groupBy=day
GET /projects/:id/funnels?steps=["signup","onboard","purchase"]&window=7d
GET /projects/:id/retention?cohortEvent=signup&returnEvent=login&periods=8w
```

## License

MIT