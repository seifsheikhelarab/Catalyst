#!/usr/bin/env bun
const services = [
  "../services/ingest-service/src/index.ts",
  "../services/validation-service/src/index.ts",
  "../services/enrichment-service/src/index.ts",
  "../services/raw-storage-service/src/index.ts",
  "../services/stream-processor-service/src/index.ts",
  "../services/project-service/src/index.ts",
  "../services/auth-service/src/index.ts",
  "../services/api-gateway/src/index.ts",
  "../services/query-api-service/src/index.ts",
  "../services/websocket-service/src/index.ts",
];

const procs = services.map((script) =>
  Bun.spawn(["bun", script], {
    cwd: import.meta.dir,
    stdout: "inherit",
    stderr: "inherit",
  }),
);

process.on("SIGINT", () => {
  procs.forEach((p) => p.kill());
  process.exit(0);
});

process.on("SIGTERM", () => {
  procs.forEach((p) => p.kill());
  process.exit(0);
});
