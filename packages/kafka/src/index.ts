import {
  Kafka,
  type Consumer,
  type Producer,
  logLevel,
  type KafkaConfig,
  type BrokersFunction,
} from "kafkajs";

export interface KafkaOptions {
  clientId?: string;
  brokers?: string[] | BrokersFunction;
  ssl?: boolean;
  sasl?: KafkaConfig["sasl"];
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
    logLevel: logLevel.WARN,
  });

  return kafkaInstance;
}

export async function getProducer(options: KafkaOptions = {}): Promise<Producer> {
  if (producerInstance) {
    return producerInstance;
  }

  const kafka = getKafka(options);
  producerInstance = kafka.producer();
  await producerInstance.connect();
  return producerInstance;
}

export async function createConsumer(
  groupId: string,
  options: KafkaOptions = {},
): Promise<Consumer> {
  const kafka = getKafka(options);
  const consumer = kafka.consumer({ groupId });
  await consumer.connect();
  return consumer;
}

export const TOPICS = {
  DEAD_LETTER: "dead-letter-events",
  ENRICHED_EVENTS: "enriched-events",
  RAW_EVENTS: "raw-events",
  VALIDATED_EVENTS: "validated-events",
} as const;

export type { Kafka, Consumer, Producer, KafkaConfig };
