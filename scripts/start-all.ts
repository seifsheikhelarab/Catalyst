#!/usr/bin/env bun
const services = [
  "../services/ingest-service/src/index.ts",
  "../services/validate-enrich-service/src/index.ts",
  "../services/stream-processor-service/src/index.ts",
  "../services/management-service/src/index.ts",
  "../services/query-api-service/src/index.ts",
  "../services/api-gateway/src/index.ts",
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
