import {
  Kafka,
  Partitioners,
  type Consumer,
  type Producer,
  type EachMessagePayload,
  type KafkaMessage,
  type KafkaConfig,
  type BrokersFunction,
  logLevel,
} from "kafkajs";

export interface KafkaOptions {
  clientId?: string;
  brokers?: string[] | BrokersFunction;
  ssl?: boolean;
  sasl?: KafkaConfig["sasl"];
}

export interface ConsumerOptions extends KafkaOptions {
  sessionTimeout?: number;
  heartbeatInterval?: number;
}

const defaultOptions: KafkaOptions = {
  clientId: "catalyst",
  brokers: [process.env.KAFKA_BROKER || "localhost:9092"],
};

let kafkaInstance: Kafka | null = null;
let producerInstance: Producer | null = null;

export function getKafka(options: KafkaOptions = {}): Kafka {
  if (kafkaInstance) {
    return kafkaInstance;
  }

  const config = { ...defaultOptions, ...options };

  kafkaInstance = new Kafka({
    brokers: config.brokers ?? ["localhost:9092"],
    clientId: config.clientId,
    ssl: config.ssl,
    sasl: config.sasl,
    connectionTimeout: parseInt(process.env.KAFKA_CONNECTION_TIMEOUT || "30000", 10),
    logLevel: logLevel.WARN,
  });

  return kafkaInstance;
}

export async function getProducer(options: KafkaOptions = {}): Promise<Producer> {
  if (producerInstance) {
    return producerInstance;
  }

  const kafka = getKafka(options);
  producerInstance = kafka.producer({
    createPartitioner: Partitioners.LegacyPartitioner,
  });
  await producerInstance.connect();
  return producerInstance;
}

export async function createConsumer(
  groupId: string,
  options: ConsumerOptions = {},
): Promise<Consumer> {
  const kafka = getKafka(options);
  const consumer = kafka.consumer({
    groupId,
    sessionTimeout: options.sessionTimeout ?? 30000,
    heartbeatInterval: options.heartbeatInterval ?? 3000,
  });
  await consumer.connect();
  return consumer;
}

export interface DLQSource {
  topic: string;
  partition: number;
  message: KafkaMessage;
}

export async function sendToDLQ(
  producer: Producer,
  source: DLQSource,
  error: unknown,
): Promise<void> {
  const { message, topic, partition } = source;
  const reason = error instanceof Error ? `${error.message}\n${error.stack}` : String(error);
  const dlqMessage = {
    originalTopic: topic,
    originalPartition: partition,
    originalOffset: message.offset,
    originalKey: message.key?.toString(),
    originalValue: message.value?.toString(),
    originalHeaders: headersToObject(message.headers),
    reason,
    timestamp: Date.now(),
  };

  await producer.send({
    topic: TOPICS.DEAD_LETTER,
    messages: [
      {
        key: message.key,
        value: JSON.stringify(dlqMessage),
        headers: {
          originalTopic: Buffer.from(topic),
          reason: Buffer.from(reason.slice(0, 500)),
        },
      },
    ],
  });
}

function headersToObject(headers: Record<string, any> | undefined): Record<string, string> {
  const out: Record<string, string> = {};
  if (!headers) return out;
  for (const [k, v] of Object.entries(headers)) {
    if (v == null) continue;
    out[k] = Buffer.isBuffer(v) ? v.toString() : String(v);
  }
  return out;
}

export const TOPICS = {
  DEAD_LETTER: "dead-letter-events",
  ENRICHED_EVENTS: "enriched-events",
  RAW_EVENTS: "raw-events",
} as const;

export type { Kafka, Consumer, Producer, KafkaConfig, EachMessagePayload, KafkaMessage };
export { DLQEnvelopeSchema, type DLQEnvelope } from "@catalyst/types";
