import pino from "pino";

const loggers = new Set<pino.Logger>();

export interface LoggerOptions {
  name?: string;
  level?: string;
  pretty?: boolean;
}

export function createLogger(options: LoggerOptions = {}) {
  const {
    name = "catalyst",
    level = "info",
    pretty = process.env.NODE_ENV !== "production",
  } = options;

  const config: pino.LoggerOptions = {
    name,
    level,
    formatters: {
      level: (label) => ({ level: label }),
    },
  };

  let logger: pino.Logger;
  if (pretty) {
    logger = pino({
      ...config,
      transport: {
        target: "pino-pretty",
        options: {
          colorize: true,
          translateTime: "SYS:standard",
          ignore: "pid,hostname",
        },
      },
    });
  } else {
    logger = pino(config);
  }

  loggers.add(logger);
  return logger;
}

export async function flushLogs(): Promise<void> {
  for (const logger of loggers) {
    await new Promise<void>((resolve, reject) => logger.flush((err) => err ? reject(err) : resolve()));
  }
}

export function sleep(ms: number): Promise<void> {
  return new Promise((r) => setTimeout(r, ms));
}
