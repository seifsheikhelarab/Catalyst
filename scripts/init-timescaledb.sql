-- TimescaleDB schema for event rollups
-- Run once via: psql $POSTGRES_URL -f scripts/init-timescaledb.sql

CREATE TABLE IF NOT EXISTS event_rollups (
  project_id   TEXT NOT NULL,
  event        TEXT NOT NULL,
  bucket       TIMESTAMPTZ NOT NULL,
  count        BIGINT NOT NULL DEFAULT 0,
  unique_users BIGINT NOT NULL DEFAULT 0,
  PRIMARY KEY (project_id, event, bucket)
);

SELECT create_hypertable('event_rollups', 'bucket', if_not_exists => TRUE, migrate_data => TRUE);

CREATE INDEX IF NOT EXISTS idx_rollups_project ON event_rollups (project_id, bucket DESC);
CREATE INDEX IF NOT EXISTS idx_rollups_event ON event_rollups (event, bucket DESC);
