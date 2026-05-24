# Catalyst - Real-Time Analytics Pipeline

## Operational Constraints

- **Do not run servers, containers, or Docker** - Ask user to start infrastructure
- **Use oxlint and oxfmt** - Not eslint/prettier (already configured in package.json)

## Project Status

Greenfield monorepo. Services exist but not yet tested. Phases 0–3 complete.

### Phase 0 - Complete

- Base `tsconfig.json` with path aliases
- Centralized event schemas in `@catalyst/types`
- ESLint + Prettier configs replaced by oxlint/oxfmt

### Phase 1 - Complete

- `ingest-service`: POST /track → raw-events Kafka topic, idempotency via Redis dedup keys
- `validation-service`: Consumes raw-events, validates with Zod, publishes to validated-events, invalid → dead-letter-events
- `enrichment-service`: Consumes validated-events, adds geo (geoip-lite), UA parsing (ua-parser-js), session stitching (Redis)

### Phase 2 - Complete

- `raw-storage-service`: Consumes enriched-events, batches (1k or 5s), bulk inserts to ClickHouse
- `stream-processor-service`: Consumes enriched-events, Redis counters (1m/1h/1d buckets), HyperLogLog unique users, hourly rollups to TimescaleDB
- Schema init scripts: `scripts/init-clickhouse.sql`, `scripts/init-timescaledb.sql`

### Phase 3 - Complete

- `project-service`: Project CRUD, API key generation (bcrypt hashed), event schemas with Redis cache
- `auth-service`: Register/login, JWT access (15min) + refresh (7d) tokens via jose, Redis revocation
- `api-gateway`: Routes `/track` (API key), `/auth/*` (public), `/api/*` (JWT), `/projects/*` (both); Redis rate limiting per API key
- Schema init: `scripts/init-phase3.sql`

### Phase 4 - Complete

- `query-api-service`: REST endpoints for metrics (TimescaleDB), funnels + retention + users (ClickHouse), live (Redis); query caching with 30s TTL

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

| Service                    | Purpose                                          |
| -------------------------- | ------------------------------------------------ |
| `ingest-service`           | Receive events, publish to Kafka, return 202     |
| `validation-service`       | Zod schema validation per project                |
| `enrichment-service`       | GeoIP, UA parsing, session stitching             |
| `stream-processor-service` | Rolling windows, funnels, retention, HyperLogLog |
| `raw-storage-service`      | Batch write to ClickHouse                        |
| `query-api-service`        | REST API for dashboards                          |
| `websocket-service`        | Push live metric updates                         |
| `project-service`          | CRUD for projects, API keys                      |
| `auth-service`             | JWT auth for dashboard                           |
| `api-gateway`              | Auth, rate limiting, routing                     |

## Kafka Topics

```
raw-events → validated-events → enriched-events → (stream-processor / raw-storage)
                                          ↳ dead-letter-events
```

## Setup

1. Monorepo with Turborepo or pnpm workspaces (Phase 0)
2. Shared packages: `@catalyst/logger`, `@catalyst/kafka`, `@catalyst/redis`, `@catalyst/types`
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

## Running Services

```bash
# Start infrastructure (YOU run this)
docker compose up

# Initialize databases (YOU run these once)
clickhouse-client --query "$(cat scripts/init-clickhouse.sql)"
docker exec -i catalyst-postgres psql -U catalyst -d catalyst < scripts/init-timescaledb.sql
docker exec -i catalyst-postgres psql -U catalyst -d catalyst < scripts/init-phase3.sql

# Run services (from monorepo root)
bun run start       # all services (gateway on 3000, ingest on 3004, project on 3001, auth on 3002)
# Or run individually:
bun run gateway     # api-gateway on port 3000
bun run project     # project-service on port 3001
bun run auth        # auth-service on port 3002
bun run ingest      # ingest-service on port 3004
bun run validate    # validation-service
bun run enrich     # enrichment-service
bun run raw-storage # raw-storage-service
bun run stream     # stream-processor-service
bun run query-api  # query-api-service
```

## Reference

- Full architecture: `project_idea.md`
- Implementation timeline: `implementation_plan.md` (9 phases, 10 weeks)
