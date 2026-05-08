import { Kafka, logLevel } from 'kafkajs';
import { z } from 'zod';
import pino from 'pino';

const logger = pino({ name: 'validation-service', level: 'info' });

const RawEventSchema = z.object({
  projectId: z.string(),
  event: z.string(),
  userId: z.string().optional(),
  timestamp: z.number(),
  properties: z.record(z.any(),z.unknown()).optional(),
  traceId: z.string().optional(),
});

type RawEvent = z.infer<typeof RawEventSchema>;

type ValidatedEvent = RawEvent & { validatedAt: number };

const TOPICS = {
  RAW_EVENTS: 'raw-events',
  VALIDATED_EVENTS: 'validated-events',
  ENRICHED_EVENTS: 'enriched-events',
  DEAD_LETTER: 'dead-letter-events',
} as const;

const CONSUMER_GROUP = 'validation-service';
const INPUT_TOPIC = TOPICS.RAW_EVENTS;
const VALIDATED_TOPIC = TOPICS.VALIDATED_EVENTS;
const DLQ_TOPIC = TOPICS.DEAD_LETTER;

const kafka = new Kafka({
  clientId: 'validation-service',
  brokers: [process.env.KAFKA_BROKER || 'localhost:9092'],
  logLevel: logLevel.WARN,
});

async function start() {
  logger.info({ topic: INPUT_TOPIC, group: CONSUMER_GROUP }, 'Starting validation service');

  const consumer = kafka.consumer({ groupId: CONSUMER_GROUP });
  await consumer.connect();

  const producer = kafka.producer();
  await producer.connect();

  await consumer.subscribe({ topic: INPUT_TOPIC, fromBeginning: false });

  await consumer.run({
    eachMessage: async ({ message }) => {
      const rawValue = message.value?.toString();
      if (!rawValue) return;

      const traceId = message.headers?.traceId?.toString() || 'unknown';

      try {
        const rawEvent = JSON.parse(rawValue) as RawEvent;

        const validationResult = RawEventSchema.safeParse(rawEvent);

        if (!validationResult.success) {
          logger.warn({ traceId, errors: validationResult.error.issues }, 'Event validation failed');

          await producer.send({
            topic: DLQ_TOPIC,
            messages: [{
              key: rawEvent.projectId,
              value: JSON.stringify({
                originalEvent: rawEvent,
                error: validationResult.error.issues,
                traceId,
                failedAt: Date.now(),
              }),
            }],
          });
          return;
        }

        const validatedEvent: ValidatedEvent = {
          ...rawEvent,
          validatedAt: Date.now(),
        };

        await producer.send({
          topic: VALIDATED_TOPIC,
          messages: [{
            key: validatedEvent.projectId,
            value: JSON.stringify(validatedEvent),
          }],
        });

        logger.info({ traceId, event: validatedEvent.event }, 'Event validated');
      } catch (err) {
        logger.error({ traceId, error: err }, 'Error processing message');
      }
    },
  });

  logger.info('Validation service running');
}

start().catch((err) => {
  logger.error({ error: err }, 'Fatal error');
  process.exit(1);
});