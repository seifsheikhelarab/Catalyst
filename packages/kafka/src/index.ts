import { Kafka, Consumer, Producer, KafkaConfig, logLevel } from 'kafkajs';

export interface KafkaOptions {
  clientId?: string;
  brokers?: string[];
  ssl?: boolean;
  sasl?: {
    mechanism: 'plain' | 'scram-sha-256' | 'scram-sha-512';
    username: string;
    password: string;
  };
}

const defaultOptions: KafkaOptions = {
  clientId: 'catalyst',
  brokers: [process.env.KAFKA_BROKER || 'localhost:9092'],
};

let kafkaInstance: Kafka | null = null;
let producerInstance: Producer | null = null;

export function getKafka(options: KafkaOptions = {}): Kafka {
  if (kafkaInstance) return kafkaInstance;

  const config = { ...defaultOptions, ...options };

  kafkaInstance = new Kafka({
    clientId: config.clientId,
    brokers: config.brokers,
    ssl: config.ssl,
    sasl: config.sasl,
    logLevel: logLevel.WARN,
  });

  return kafkaInstance;
}

export async function getProducer(options: KafkaOptions = {}): Promise<Producer> {
  if (producerInstance) return producerInstance;

  const kafka = getKafka(options);
  producerInstance = kafka.producer();
  await producerInstance.connect();
  return producerInstance;
}

export async function createConsumer(groupId: string, options: KafkaOptions = {}): Promise<Consumer> {
  const kafka = getKafka(options);
  const consumer = kafka.consumer({ groupId });
  await consumer.connect();
  return consumer;
}

export const TOPICS = {
  RAW_EVENTS: 'raw-events',
  VALIDATED_EVENTS: 'validated-events',
  ENRICHED_EVENTS: 'enriched-events',
  DEAD_LETTER: 'dead-letter-events',
} as const;

export { Kafka, Consumer, Producer };
export type { KafkaConfig };