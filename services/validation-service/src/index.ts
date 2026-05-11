import { getKafka, getProducer, TOPICS } from "@catalyst/kafka";
import { RawEventSchema, type RawEvent, type ValidatedEvent } from "@catalyst/types";
import { createLogger } from "@catalyst/logger";
import crypto from "crypto";

const logger = createLogger({ name: "validation-service" });

const CONSUMER_GROUP = "validation-service";
const INPUT_TOPIC = TOPICS.RAW_EVENTS;
const VALIDATED_TOPIC = TOPICS.VALIDATED_EVENTS;
const DLQ_TOPIC = TOPICS.DEAD_LETTER;

const kafka = getKafka({ clientId: "validation-service" });

let consumer: any;
let producer: any;

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
