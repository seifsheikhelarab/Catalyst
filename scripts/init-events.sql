-- Events table (TimescaleDB hypertable, replaces ClickHouse events)

CREATE TABLE IF NOT EXISTS events (
  id BIGSERIAL,
  project_id TEXT NOT NULL,
  event TEXT NOT NULL,
  user_id TEXT NOT NULL DEFAULT '',
  session_id TEXT NOT NULL DEFAULT '',
  country TEXT NOT NULL DEFAULT '',
  city TEXT NOT NULL DEFAULT '',
  device_type TEXT NOT NULL DEFAULT '',
  browser TEXT NOT NULL DEFAULT '',
  os TEXT NOT NULL DEFAULT '',
  properties JSONB DEFAULT '{}',
  timestamp TIMESTAMPTZ NOT NULL,
  ingested_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

SELECT create_hypertable('events', 'timestamp', if_not_exists => TRUE);

CREATE INDEX IF NOT EXISTS idx_events_project_event
  ON events (project_id, event, timestamp DESC);

CREATE INDEX IF NOT EXISTS idx_events_user
  ON events (user_id, timestamp DESC);
