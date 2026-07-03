# E2E Test Guide

1. Start Everything

```bash
docker compose up -d
```

This starts Redpanda (Kafka), Redis, PostgreSQL/TimescaleDB, Jaeger, Prometheus, Grafana, Loki, and all 6 Catalyst services. DB init scripts auto-apply on first start.

2. End-to-End Test Flow

### A — Registration & Auth

```bash
# Register
curl -X POST http://localhost:3000/auth/register \
  -H "Content-Type: application/json" \
  -d '{"email":"test@example.com","password":"password123"}'

# Save from response
TOKEN="eyJ..."   # accessToken
REFRESH="eyJ..." # refreshToken
ORG_ID="..."
USER_ID="..."

# Login (alternative to register)
# Refresh token
curl -X POST http://localhost:3000/auth/refresh \
  -H "Content-Type: application/json" \
  -d '{"refreshToken":"'$REFRESH'"}'

# Get current user
curl -H "Authorization: Bearer $TOKEN" http://localhost:3000/auth/me

# Logout
curl -X POST http://localhost:3000/auth/logout \
  -H "Content-Type: application/json" \
  -d '{"refreshToken":"'$REFRESH'"}'
```

### B — Project & API Key Management

```bash
# Create a project
curl -X POST http://localhost:3000/projects \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer $TOKEN" \
  -d "{\"name\":\"My App\",\"orgId\":\"$ORG_ID\"}"

PROJECT_ID="..."

# Get project by ID
curl -H "Authorization: Bearer $TOKEN" http://localhost:3000/projects/$PROJECT_ID

# List projects
curl -H "Authorization: Bearer $TOKEN" http://localhost:3000/projects?orgId=$ORG_ID

# Create an API key
curl -X POST http://localhost:3000/projects/$PROJECT_ID/keys \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer $TOKEN"

API_KEY="pk_live_..."

# List API keys
curl -H "Authorization: Bearer $TOKEN" http://localhost:3000/projects/$PROJECT_ID/keys
```

### C — Event Schema Management

```bash
# Create a schema for an event
curl -X POST http://localhost:3000/projects/$PROJECT_ID/schemas \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer $TOKEN" \
  -d '{
    "eventName": "page_view",
    "schema": {
      "type": "object",
      "required": ["url"],
      "properties": {
        "url": {"type": "string"},
        "referrer": {"type": "string"}
      }
    }
  }'

# List schemas
curl -H "Authorization: Bearer $TOKEN" http://localhost:3000/projects/$PROJECT_ID/schemas

# Get schema by event name
curl -H "Authorization: Bearer $TOKEN" http://localhost:3000/projects/$PROJECT_ID/schemas/page_view
```

### D — Event Pipeline (Track → Process → Store)

```bash
# Send a valid event through the gateway
curl -X POST http://localhost:3000/track \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer $API_KEY" \
  -d '{
    "event": "page_view",
    "userId": "user_abc123",
    "properties": {
      "url": "/pricing",
      "userAgent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
    },
    "timestamp": '$(date +%s)'000
  }'
```

### E — Pipeline Verification

```bash
# Check Kafka topics via Redpanda Console
open http://localhost:8080

# Check raw events in TimescaleDB
docker exec catalyst-postgres psql -U catalyst -d catalyst -c "SELECT count(*) FROM events"

# Check rollups
docker exec catalyst-postgres psql -U catalyst -d catalyst -c "SELECT * FROM event_rollups ORDER BY bucket DESC LIMIT 10"
```

### F — Query API

```bash
# Metrics
curl -H "Authorization: Bearer $TOKEN" \
  "http://localhost:3000/api/projects/$PROJECT_ID/metrics?event=page_view&from=$(date -d '-1 hour' +%Y-%m-%dT%H:%M:%SZ)&to=$(date +%Y-%m-%dT%H:%M:%SZ)"

# Funnels
curl -H "Authorization: Bearer $TOKEN" \
  "http://localhost:3000/api/projects/$PROJECT_ID/funnels?steps=[\"page_view\",\"signup\"]&window=7d"

# Retention
curl -H "Authorization: Bearer $TOKEN" \
  "http://localhost:3000/api/projects/$PROJECT_ID/retention?cohortEvent=page_view&returnEvent=page_view&periods=4"

# Users
curl -H "Authorization: Bearer $TOKEN" \
  "http://localhost:3000/api/projects/$PROJECT_ID/users?limit=10"

# Live counters
curl -H "Authorization: Bearer $TOKEN" \
  "http://localhost:3000/api/projects/$PROJECT_ID/events/live"
```

### G — DLQ Flow

```bash
# Send invalid event (bypasses gateway validation)
curl -s -X POST http://localhost:3004/track \
  -H "Content-Type: application/json" \
  -d '{"projectId":"bad","timestamp":1}' | jq .
# → 202

# List DLQ events
curl -H "Authorization: Bearer $TOKEN" "http://localhost:3000/admin/dlq"

# Get DLQ event by ID
DLQ_ID=1
curl -H "Authorization: Bearer $TOKEN" "http://localhost:3000/admin/dlq/$DLQ_ID"

# Retry a DLQ event
curl -X POST -H "Authorization: Bearer $TOKEN" "http://localhost:3000/admin/dlq/$DLQ_ID/retry"

# Discard a DLQ event
curl -X POST -H "Authorization: Bearer $TOKEN" "http://localhost:3000/admin/dlq/$DLQ_ID/discard"
```

### H — Duplicate Dedup

```bash
# Send same event twice → second returns "duplicate"
curl -s -X POST http://localhost:3000/track \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer $API_KEY" \
  -d "{\"event\":\"test\",\"projectId\":\"$PROJECT_ID\",\"timestamp\":$(date +%s)000}" | jq .
# → {"status":"duplicate"}
```

### I — WebSocket Live Updates

```bash
# Install wscat if needed
npm install -g wscat

# Connect to live updates
wscat -c "ws://localhost:3000/live?token=$TOKEN&projectId=$PROJECT_ID"

# In another terminal, send events — you'll see live count updates
```

3. Monitoring Dashboards

- Grafana: http://localhost:3030 (pipeline-overview dashboard with 12 panels)
- Jaeger traces: http://localhost:16686 (search by service "ingest-service")
- Redpanda Console: http://localhost:8080 (Kafka topic inspection)
- Prometheus: http://localhost:9090 (raw metrics)
