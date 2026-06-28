# E2E Test Guide

1. Start Infrastructure
// bash
docker compose up -d
This starts Redpanda (Kafka), Redis, PostgreSQL/TimescaleDB, ClickHouse, Jaeger, Prometheus, Grafana, Loki.
2. Initialize Databases
// bash

# ClickHouse

docker exec -i catalyst-clickhouse clickhouse-client --user catalyst --password catalyst --query "$(cat scripts/init-clickhouse.sql)"

# TimescaleDB

docker exec -i catalyst-postgres psql -U catalyst -d catalyst < scripts/init-timescaledb.sql

# Project/Auth tables

docker exec -i catalyst-postgres psql -U catalyst -d catalyst < scripts/init-phase3.sql

# DLQ tables

docker exec -i catalyst-postgres psql -U catalyst -d catalyst < scripts/init-phase8.sql
3. Install & Start Services
// bash
bun install
bun run start   # starts all 11 services
4. End-to-End Test Flow
Step A — Register a user & create a project:
// bash

# Register

curl -X POST <http://localhost:3000/auth/register> \
  -H "Content-Type: application/json" \
  -d '{"email":"<test@example.com>","password":"password123"}'

# Save the accessToken from the response

TOKEN="..."
ORG_ID="..."

# Create a project

curl -X POST <http://localhost:3000/projects> \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer $TOKEN" \
  -d "{\"name\":\"My App\",\"orgId\":\"$ORG_ID\"}"

# Save the projectId

PROJECT_ID="..."

# Create an API key

curl -X POST <http://localhost:3000/projects/$PROJECT_ID/keys> \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer $TOKEN"

# Save the raw API key (starts with pk_live_)

API_KEY="pk_live_..."
Step B — Send test events through the pipeline:
// bash

# Single event

curl -X POST <http://localhost:3000/track> \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer $API_KEY" \
  -d "{
    \"event\": \"page_view\",
    \"userId\": \"user_abc123\",
    \"properties\": {
      \"url\": \"/pricing\",
      \"userAgent\": \"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36\"
    },
    \"timestamp\": $(date +%s)000
  }"
Step C — Verify at each pipeline stage:
// bash

# 1. Check Kafka topics via Redpanda Console

open <http://localhost:8080>

# 2. Query ClickHouse for stored events

docker exec -i catalyst-clickhouse clickhouse-client --user catalyst --password catalyst --query "SELECT count(*) FROM catalyst.events"

# 3. Check rollups in TimescaleDB

docker exec -i catalyst-postgres psql -U catalyst -d catalyst -c "SELECT * FROM event_rollups ORDER BY bucket DESC LIMIT 10"

# 4. Query API

curl -H "Authorization: Bearer $TOKEN" "http://localhost:3000/api/projects/$PROJECT_ID/metrics?event=page_view&from=$(date -d '-1 hour' +%Y-%m-%dT%H:%M:%SZ)&to=$(date +%Y-%m-%dT%H:%M:%SZ)"
Step D — Duplicate dedup test:
// bash

# Send same event twice → second returns "duplicate"

curl -s -X POST <http://localhost:3000/track> \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer $API_KEY" \
  -d "{\"event\":\"test\",\"projectId\":\"$PROJECT_ID\",\"timestamp\":$(date +%s)000}" | jq .

# → {"status":"duplicate"}

Step E — Send invalid event (triggers DLQ):
// bash

# Missing required fields

curl -s -X POST <http://localhost:3000/track> \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer $API_KEY" \
  -d '{"bad":"data"}' | jq .

# → 400 Bad Request

# Send directly to ingest to bypass gateway validation

curl -s -X POST <http://localhost:3004/track> \
  -H "Content-Type: application/json" \
  -d '{"projectId":"bad","timestamp":1}' | jq .

# → 202 (bypasses API key check)

# Then check DLQ

curl -H "Authorization: Bearer $TOKEN" "<http://localhost:3000/admin/dlq>"
Step F — WebSocket live test:
// bash

# Install wscat if needed

npm install -g wscat

# Connect to live updates

wscat -c "ws://localhost:3000/live?token=$TOKEN&projectId=$PROJECT_ID"

# In another terminal, send events — you'll see live count updates

1. Monitoring Dashboards

- Grafana:  <http://localhost:3030>  (pipeline-overview dashboard with 12 panels)
- Jaeger traces:  <http://localhost:16686>  (search by service  ingest-service )
- Redpanda Console:  <http://localhost:8080>  (Kafka topic inspection)
- Prometheus:  <http://localhost:9090>  (raw metrics)
