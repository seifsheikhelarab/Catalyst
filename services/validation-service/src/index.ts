import { getKafka, getProducer, TOPICS, type Consumer, type Producer } from "@catalyst/kafka";
import { RawEventSchema, type RawEvent, type ValidatedEvent } from "@catalyst/types";
import { createLogger } from "@catalyst/logger";
import { initTracing, startSpanWithTraceContext } from "@catalyst/tracing";
import { createCounter, metricsHandler } from "@catalyst/metrics";
import crypto from "crypto";

const logger = createLogger({ name: "validation-service" });

const eventsValid = createCounter({ name: "validation_events_valid_total", help: "Valid events forwarded" });
const eventsRejected = createCounter({ name: "validation_events_rejected_total", help: "Events rejected by schema" });

const CONSUMER_GROUP = "validation-service";
const INPUT_TOPIC = TOPICS.RAW_EVENTS;
const VALIDATED_TOPIC = TOPICS.VALIDATED_EVENTS;
const DLQ_TOPIC = TOPICS.DEAD_LETTER;

const kafka = getKafka({ clientId: "validation-service" });

let consumer: Consumer;
let producer: Producer;

async function shutdown() {
  logger.info("Shutting down...");
  await consumer?.stop();
  await consumer?.disconnect();
  await producer?.disconnect();
  process.exit(0);
}

process.on("SIGTERM", shutdown);
process.on("SIGINT", shutdown);

async function start() {
  initTracing({ serviceName: "validation-service" });
  logger.info({ topic: INPUT_TOPIC, group: CONSUMER_GROUP }, "Starting validation service");

  consumer = kafka.consumer({ groupId: CONSUMER_GROUP });
  await consumer.connect();

  producer = await getProducer();

  await consumer.subscribe({ topic: INPUT_TOPIC, fromBeginning: false });

  await consumer.run({
    eachMessage: async ({ message }) => {
      const rawValue = message.value?.toString();
      if (!rawValue) return;

      const traceId = message.headers?.traceId?.toString() || crypto.randomUUID();

      try {
        const rawEvent = JSON.parse(rawValue) as RawEvent;

        const span = startSpanWithTraceContext({ traceparent: message.headers?.traceparent?.toString() }, "validation.process", { "event.projectId": rawEvent?.projectId });

        const validationResult = RawEventSchema.safeParse(rawEvent);

        if (!validationResult.success) {
          logger.warn(
            { traceId, errors: validationResult.error.issues },
            "Event validation failed",
          );

          await producer.send({
            topic: DLQ_TOPIC,
            messages: [
              {
                key: rawEvent.projectId,
                value: JSON.stringify({
                  originalEvent: rawEvent,
                  reason: "Schema validation failed",
                  error: JSON.stringify(validationResult.error.issues),
                  timestamp: Date.now(),
                }),
                headers: { traceId },
              },
            ],
          });
          eventsRejected.inc();
          span.end();
          return;
        }

        const validatedEvent: ValidatedEvent = {
          ...rawEvent,
          validatedAt: Date.now(),
        };

        await producer.send({
          topic: VALIDATED_TOPIC,
          messages: [
            {
              key: validatedEvent.projectId,
              value: JSON.stringify(validatedEvent),
              headers: { traceId },
            },
          ],
        });

        eventsValid.inc();
        span.end();

        logger.info({ traceId, event: validatedEvent.event }, "Event validated");
      } catch (err) {
        logger.error({ traceId, error: err }, "Error processing message");
      }
    },
  });

  logger.info("Validation service running");
}

start().catch((err) => {
  logger.error({ error: err }, "Fatal error");
  process.exit(1);
});
