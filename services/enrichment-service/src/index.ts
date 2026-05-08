import { Kafka, logLevel } from "kafkajs";
import Redis from "ioredis";
import { type EnrichedEvent, type ValidatedEvent } from "@catalyst/types";
import pino from "pino";
import crypto from "crypto";
import UASparser from "ua-parser-js";

const logger = pino({ level: "info", name: "enrichment-service" });

const TOPICS = {
  DEAD_LETTER: "dead-letter-events",
  ENRICHED_EVENTS: "enriched-events",
  RAW_EVENTS: "raw-events",
  VALIDATED_EVENTS: "validated-events",
} as const;

const CONSUMER_GROUP = "enrichment-service";
const INPUT_TOPIC = TOPICS.VALIDATED_EVENTS;
const OUTPUT_TOPIC = TOPICS.ENRICHED_EVENTS;
const SESSION_TTL = 1800;

const kafka = new Kafka({
  brokers: [process.env.KAFKA_BROKER || "localhost:9092"],
  clientId: "enrichment-service",
  logLevel: logLevel.WARN,
});

const redis = new Redis({
  host: process.env.REDIS_HOST || "localhost",
  port: parseInt(process.env.REDIS_PORT || "6379"),
});

const UAParser = UASparser.UAParser;

async function getOrCreateSession(projectId: string, userId: string): Promise<string> {
  const sessionKey = `session:${projectId}:${userId}`;
  const existingSession = await redis.get(sessionKey);
  if (existingSession) {
    return existingSession;
  }

  const newSessionId = crypto.randomUUID();
  await redis.set(sessionKey, newSessionId, "EX", SESSION_TTL);
  return newSessionId;
}

function parseUserAgent(userAgent?: string) {
  if (!userAgent) {
    return { browser: "unknown", deviceType: "unknown", os: "unknown" };
  }
  const parser = new UAParser(userAgent);
  const ua = parser.getResult();
  return {
    browser: ua.browser.name || "unknown",
    deviceType: ua.device.type || "desktop",
    os: ua.os.name || "unknown",
  };
}

async function start() {
  logger.info({ topic: INPUT_TOPIC, group: CONSUMER_GROUP }, "Starting enrichment service");

  const consumer = kafka.consumer({ groupId: CONSUMER_GROUP });
  await consumer.connect();

  const producer = kafka.producer();
  await producer.connect();

  await consumer.subscribe({ topic: INPUT_TOPIC, fromBeginning: false });

  await consumer.run({
    eachMessage: async (payload) => {
      const message = payload.message;
      const rawValue = message.value?.toString();
      if (!rawValue) return;

      const traceId = message.headers?.traceId?.toString() || "unknown";

      try {
        const validatedEvent = JSON.parse(rawValue) as ValidatedEvent;

        const { deviceType, browser, os } = parseUserAgent(
          validatedEvent.properties?.userAgent as string,
        );

        const sessionId = validatedEvent.userId
          ? await getOrCreateSession(validatedEvent.projectId, validatedEvent.userId)
          : undefined;

        const enrichedEvent: EnrichedEvent = {
          ...validatedEvent,
          deviceType,
          browser,
          os,
          sessionId,
          enrichedAt: Date.now(),
        };

        await producer.send({
          topic: OUTPUT_TOPIC,
          messages: [
            {
              key: enrichedEvent.projectId,
              value: JSON.stringify(enrichedEvent),
            },
          ],
        });

        logger.info({ traceId, event: enrichedEvent.event, sessionId }, "Event enriched");
      } catch (err) {
        logger.error({ traceId, error: err }, "Error processing message");
      }
    },
  });

  logger.info("Enrichment service running");
}

start().catch((err) => {
  logger.error({ error: err }, "Fatal error");
  process.exit(1);
});
