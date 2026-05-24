CREATE DATABASE IF NOT EXISTS catalyst;

CREATE TABLE IF NOT EXISTS catalyst.events (
  project_id    String,
  event         String,
  user_id       String,
  session_id    String,
  country       String,
  city          String,
  device_type   String,
  browser       String,
  os            String,
  timestamp     DateTime64(3)
)
ENGINE = MergeTree()
PARTITION BY toYYYYMM(timestamp)
ORDER BY (project_id, event, timestamp)
TTL toDateTime(timestamp) + INTERVAL 90 DAY;
