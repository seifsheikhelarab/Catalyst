# Catalyst - Real-Time Analytics Pipeline

## Operational Constraints

- **Do not run servers, containers, or Docker** - Ask user to start infrastructure
- **Use oxlint and oxfmt** - Not eslint/prettier (already configured in package.json)

## Project Status

Greenfield monorepo. Architecture consolidated from 11 services to 6.

### Current Services

- **ingest-service** (port 3004): POST /track → raw-events Kafka topic, Redis dedup
- **validate-enrich-service** (metrics 9101): Consumes raw-events, validates with Zod, adds GeoIP/UA/session, publishes to enriched-events, invalid → dead-letter-events
- **stream-processor-service** (metrics 9103): Consumes enriched-events, Redis counters + HLL, hourly rollups to TimescaleDB, raw event buffer to TimescaleDB events hypertable, Redis Pub/Sub live updates
- **management-service** (port 3001): Auth (register/login/JWT), project CRUD + API keys, DLQ consumer + admin endpoints
- **query-api-service** (port 3003): REST endpoints for metrics/rollups, funnels, retention, users (all via TimescaleDB), live counters via Redis
- **api-gateway** (port 3000): Routes /track (API key), /auth/* /projects/* /admin/* /api/* (JWT), WS /live (JWT + direct Redis Pub/Sub broadcast), rate limiting

### Shared Packages

- `@catalyst/logger`, `@catalyst/kafka`, `@catalyst/redis`, `@catalyst/types`
- `@catalyst/tracing`: OpenTelemetry BatchSpanProcessor, traceparent through Kafka headers
- `@catalyst/metrics`: prom-client metrics
- `@catalyst/circuit-breaker`: opossum-based circuit breaker for TimescaleDB writes
- `@catalyst/sdk`: Browser SDK for event tracking

## Architecture

```
Client SDK → API Gateway → Ingest Service → Kafka (raw-events)
                                    ↓
                         Validate-Enrich Service → Kafka (enriched-events)
                                    ↓
                         Stream Processor (Redis + TimescaleDB)
                                    ↓
                         Query API Service → Dashboard
                         API Gateway WS → Live Updates
```

## Stack

- **Runtime:** TypeScript / Bun
- **Message Broker:** Kafka (Redpanda locally)
- **Cache:** Redis
- **Time-series / OLAP:** TimescaleDB (PostgreSQL extension)

## Kafka Topics

```
raw-events → enriched-events → dead-letter-events
```

## Setup

1. `bun install` from monorepo root
2. `docker compose up` starts everything: Redpanda, Redis, TimescaleDB, Redpanda Console, Jaeger, Prometheus, Grafana, Loki, and all 6 services. DB init scripts auto-apply on first start via `/docker-entrypoint-initdb.d/`.

## Key Patterns

- **Consumer groups** with manual offset commit (eachBatch, autoCommit: false)
- **Retry-then-DLQ** pattern (3x exponential backoff) in validate-enrich, stream-processor
- **Circuit breakers** around TimescaleDB writes (stream-processor)
- **Graceful shutdown**: drain in-flight, flush buffers, then exit (25s deadline)
- **Distributed tracing**: OpenTelemetry with traceparent propagated through Kafka headers

## Running Services

```bash
docker compose up      # everything (infra + all 6 services)
bun run start          # all 6 services locally (if running infra separately)
bun run ingest         # ingest-service only
bun run validate-enrich
bun run stream
bun run management
bun run query-api
bun run gateway
```

## Reference

- Full architecture: `project_idea.md`
- Implementation plan: `implementation_plan.md`
