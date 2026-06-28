-- Phase 8: dead-letter queue persistence
-- Run via: docker exec -i catalyst-postgres psql -U catalyst -d catalyst < scripts/init-phase8.sql

CREATE TABLE IF NOT EXISTS dlq_events (
  id                 BIGSERIAL PRIMARY KEY,
  original_topic     TEXT NOT NULL,
  original_partition INTEGER,
  original_offset    TEXT,
  original_key       TEXT,
  original_value     TEXT,
  original_headers   JSONB,
  reason             TEXT NOT NULL,
  status             TEXT NOT NULL DEFAULT 'pending' CHECK (status IN ('pending', 'retrying', 'retried', 'discarded')),
  retry_count        INTEGER NOT NULL DEFAULT 0,
  last_error         TEXT,
  created_at         TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at         TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_dlq_events_status ON dlq_events(status);
CREATE INDEX IF NOT EXISTS idx_dlq_events_created_at ON dlq_events(created_at DESC);
CREATE INDEX IF NOT EXISTS idx_dlq_events_topic ON dlq_events(original_topic);
