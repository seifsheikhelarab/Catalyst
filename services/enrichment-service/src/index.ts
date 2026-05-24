import { getKafka, getProducer, TOPICS, type Consumer, type Producer } from "@catalyst/kafka";
import { connectRedis } from "@catalyst/redis";
import { type EnrichedEvent, type ValidatedEvent } from "@catalyst/types";
import { createLogger } from "@catalyst/logger";
import { initTracing, startSpanWithTraceContext } from "@catalyst/tracing";
import { createCounter, metricsHandler } from "@catalyst/metrics";
import crypto from "crypto";
import { UAParser } from "ua-parser-js";
import geoip from "geoip-lite";

const logger = createLogger({ name: "enrichment-service" });

const eventsProcessed = createCounter({ name: "enrichment_events_processed_total", help: "Events enriched and forwarded" });

const CONSUMER_GROUP = "enrichment-service";
const INPUT_TOPIC = TOPICS.VALIDATED_EVENTS;
const OUTPUT_TOPIC = TOPICS.ENRICHED_EVENTS;
const SESSION_TTL = 1800;

const kafka = getKafka({ clientId: "enrichment-service" });
let consumer: Consumer;
let producer: Producer;
let redis: any;

async function shutdown() {
  logger.info("Shutting down...");
  await consumer?.stop();
  await consumer?.disconnect();
  await producer?.disconnect();
  await redis?.quit();
  process.exit(0);
}

process.on("SIGTERM", shutdown);
process.on("SIGINT", shutdown);

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
  initTracing({ serviceName: "enrichment-service" });
  logger.info({ topic: INPUT_TOPIC, group: CONSUMER_GROUP }, "Starting enrichment service");

  redis = await connectRedis();
  consumer = kafka.consumer({ groupId: CONSUMER_GROUP });
  await consumer.connect();

  producer = await getProducer();

  await consumer.subscribe({ topic: INPUT_TOPIC, fromBeginning: false });

  await consumer.run({
    eachMessage: async (payload) => {
      const message = payload.message;
      const rawValue = message.value?.toString();
      if (!rawValue) return;

      const traceId = message.headers?.traceId?.toString() || "unknown";

      try {
        const validatedEvent = JSON.parse(rawValue) as ValidatedEvent;

        const span = startSpanWithTraceContext({ traceparent: message.headers?.traceparent?.toString() }, "enrichment.process");
        const { deviceType, browser, os } = parseUserAgent(
          validatedEvent.properties?.userAgent as string,
        );

        const ip =
          (validatedEvent.properties?.ip as string) ||
          (validatedEvent.properties?.clientIp as string);
        let country: string | undefined;
        let city: string | undefined;
        if (ip) {
          const geo = geoip.lookup(ip);
          if (geo) {
            country = geo.country;
            city = geo.city;
          }
        }

        const sessionId = validatedEvent.userId
          ? await getOrCreateSession(validatedEvent.projectId, validatedEvent.userId)
          : undefined;

        const enrichedEvent: EnrichedEvent = {
          ...validatedEvent,
          country,
          city,
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

        eventsProcessed.inc();
        span.end();

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
