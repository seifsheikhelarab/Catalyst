import { Hono } from 'hono';
import { Kafka, logLevel } from 'kafkajs';
import Redis from 'ioredis';
import { z } from 'zod';
import pino from 'pino';
import crypto from 'crypto';

const logger = pino({
  name: 'ingest-service',
  level: 'info',
});

const RawEventSchema = z.object({
  projectId: z.string(),
  event: z.string(),
  userId: z.string().optional(),
  timestamp: z.number(),
  properties: z.record(z.unknown()).optional(),
});

type RawEvent = z.infer<typeof RawEventSchema>;

const TOPICS = {
  RAW_EVENTS: 'raw-events',
  VALIDATED_EVENTS: 'validated-events',
  ENRICHED_EVENTS: 'enriched-events',
  DEAD_LETTER: 'dead-letter-events',
} as const;

const INGRESS_TOPIC = TOPICS.RAW_EVENTS;
const DEDUP_TTL = 60;

const kafka = new Kafka({
  clientId: 'ingest-service',
  brokers: [process.env.KAFKA_BROKER || 'localhost:9092'],
  logLevel: logLevel.WARN,
});

let producer: any = null;

async function getProducer() {
  if (!producer) {
    producer = kafka.producer();
    await producer.connect();
  }
  return producer;
}

const redis = new Redis({
  host: process.env.REDIS_HOST || 'localhost',
  port: parseInt(process.env.REDIS_PORT || '6379'),
});

const app = new Hono();

async function computeDedupKey(event: RawEvent): Promise<string> {
  const hashInput = `${event.projectId}:${event.userId || ''}:${event.timestamp}:${event.event}`;
  return `dedup:${crypto.createHash('sha256').update(hashInput).digest('hex')}`;
}

app.post('/track', async (c) => {
  const body = await c.req.json();

  const parseResult = RawEventSchema.safeParse(body);

  if (!parseResult.success) {
    return c.json({ error: 'Invalid payload', details: parseResult.error.issues }, 400);
  }

  const event = parseResult.data as RawEvent;

  const dedupKey = await computeDedupKey(event);

  const existing = await redis.get(dedupKey);
  if (existing) {
    logger.info({ dedupKey, event: event.event }, 'Duplicate event rejected');
    return c.json({ status: 'duplicate' }, 202);
  }

  const prod = await getProducer();
  const traceId = crypto.randomUUID();

  const message = {
    ...event,
    traceId,
  };

  await prod.send({
    topic: INGRESS_TOPIC,
    messages: [
      {
        key: event.projectId,
        value: JSON.stringify(message),
        headers: {
          traceId,
        },
      },
    ],
  });

  await redis.set(dedupKey, '1', 'EX', DEDUP_TTL);

  logger.info({ event: event.event, projectId: event.projectId, traceId }, 'Event published');

  return c.json({ status: 'accepted', traceId }, 202);
});

app.get('/health', (c) => c.json({ status: 'ok' }));

const port = parseInt(process.env.PORT || '3000');

export default {
  port,
  fetch: app.fetch,
};