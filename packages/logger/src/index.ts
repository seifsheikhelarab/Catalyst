import pino from "pino";

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

  if (pretty) {
    return pino({
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
  }

  return pino(config);
}
