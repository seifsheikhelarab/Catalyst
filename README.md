# Catalyst - Real-Time Analytics Pipeline

A streaming-first product analytics platform. Ingests events, validates/enriches, processes in real-time, and serves live dashboards with rolling windows, funnels, and retention.

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

| Component      | Technology                         |
| -------------- | ---------------------------------- |
| Runtime        | TypeScript / Bun                   |
| Message Broker | Kafka (Redpanda locally)           |
| Cache          | Redis                              |
| Time-series    | TimescaleDB (PostgreSQL extension) |

## Services

| Service                    | Purpose                                        |
| -------------------------- | ---------------------------------------------- |
| `api-gateway`              | Auth, rate limiting, routing, WS live updates  |
| `ingest-service`           | Receive events, Redis dedup, publish to Kafka  |
| `validate-enrich-service`  | Zod validation, GeoIP, UA parsing, session     |
| `stream-processor-service` | Rolling windows, funnels, retention, rollups   |
| `management-service`       | Auth, project CRUD, API keys, schemas, DLQ     |
| `query-api-service`        | REST API for metrics, funnels, retention       |

## Quick Start

```bash
bun install
docker compose up -d
```

This starts Redpanda, Redis, TimescaleDB, and all 6 services. DB init scripts auto-apply.

## E2E Tests

```bash
bun test
```

Requires `docker compose up -d` running. See `E2E_Test.md` for the full flow reference.

## Kafka Topics

```
raw-events → enriched-events → dead-letter-events
```

## Key Patterns

- **Consumer groups** with manual offset commit (eachBatch, autoCommit: false)
- **Retry-then-DLQ** (3x exponential backoff) in validate-enrich, stream-processor
- **Circuit breakers** around TimescaleDB writes (stream-processor)
- **Graceful shutdown**: drain in-flight, flush buffers, then exit (25s deadline)
- **Distributed tracing**: OpenTelemetry with traceparent through Kafka headers
- **Live updates**: API gateway broadcasts Redis Pub/Sub via WebSocket

## Monitoring

| Tool     | URL                              |
| -------- | -------------------------------- |
| Grafana  | http://localhost:3030            |
| Jaeger   | http://localhost:16686           |
| Redpanda | http://localhost:8080            |
| Prometheus | http://localhost:9090          |

## License

MIT
