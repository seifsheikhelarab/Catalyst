# Catalyst - Real-Time Analytics Pipeline

## Project Status

Greenfield monorepo. No implementation code exists yet - only documentation in `project_idea.md` and `implementation_plan.md`.

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

- **Runtime:** TypeScript / Bun
- **Message Broker:** Kafka (Redpanda locally, Kafka for prod)
- **Cache:** Redis
- **Time-series:** TimescaleDB (PostgreSQL extension)
- **OLAP:** ClickHouse
- **Relational:** PostgreSQL

## Services to Build

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

## Kafka Topics

```
raw-events → validated-events → enriched-events → (stream-processor / raw-storage)
                                          ↳ dead-letter-events
```

## Setup

1. Monorepo with Turborepo or pnpm workspaces (Phase 0)
2. Shared packages: `@app/logger`, `@app/kafka`, `@app/redis`, `@app/types`
3. `docker compose up` starts: Redpanda, Redis, PostgreSQL, TimescaleDB, ClickHouse, Redpanda Console

## Key Patterns

- **Back-pressure handling** in ingest-service (fire-and-forget)
- **Idempotency keys** in Redis with 60s TTL for dedup
- **Consumer groups** with manual offset commit (not auto-commit)
- **Circuit breakers** around ClickHouse calls (Phase 8)
- **Graceful shutdown**: drain in-flight, flush buffers, then exit
- **Distributed tracing**: OpenTelemetry with traceparent propagated through Kafka headers

## Entry Points

This is a greenfield project. Start with Phase 0 of `implementation_plan.md`:
- Initialize monorepo structure
- Set up shared packages
- Create Docker Compose for local infrastructure

## Reference

- Full architecture: `project_idea.md`
- Implementation timeline: `implementation_plan.md` (9 phases, 10 weeks)