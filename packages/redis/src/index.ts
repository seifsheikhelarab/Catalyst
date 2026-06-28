import Redis from "ioredis";

export type RedisClient = Redis;

export interface RedisOptions {
  host?: string;
  port?: number;
  password?: string;
  db?: number;
}

const defaultOptions: RedisOptions = {
  host: process.env.REDIS_HOST || "localhost",
  port: parseInt(process.env.REDIS_PORT || "6379"),
  password: process.env.REDIS_PASSWORD,
  db: parseInt(process.env.REDIS_DB || "0"),
};

let redisInstance: Redis | null = null;

export function getRedis(options: RedisOptions = {}): Redis {
  if (redisInstance) return redisInstance;

  const config = { ...defaultOptions, ...options };
  redisInstance = new Redis({
    host: config.host,
    port: config.port,
    password: config.password,
    db: config.db,
    maxRetriesPerRequest: 3,
    lazyConnect: true,
  });

  redisInstance.on("error", (err) => {
    console.error("Redis connection error:", err);
  });

  return redisInstance;
}

export async function connectRedis(options: RedisOptions = {}): Promise<Redis> {
  const redis = getRedis(options);
  await redis.connect();
  return redis;
}

export async function disconnectRedis(): Promise<void> {
  if (redisInstance) {
    await redisInstance.quit();
    redisInstance = null;
  }
}


