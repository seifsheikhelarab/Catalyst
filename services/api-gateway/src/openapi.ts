import { apiReference } from "@scalar/hono-api-reference";
import type { Hono, Context, Env } from "hono";

const spec = {
  openapi: "3.0.3",
  info: {
    title: "Catalyst Analytics API",
    version: "1.0.0",
    description:
      "Real-time event analytics pipeline. Track events, run funnel/retention analysis, and query live metrics.\n\n**Auth flows:**\n- **API Key** (`pk_live_*`): Used for `/track` endpoints. Pass as `Authorization: Bearer <key>`.\n- **JWT**: Used for management and query endpoints. Obtain via `POST /auth/login`. Pass as `Authorization: Bearer <token>`.",
  },
  servers: [{ url: "http://localhost:3000", description: "Local development" }],
  paths: {
    "/auth/register": {
      post: {
        tags: ["Auth"],
        summary: "Register a new organization and user",
        requestBody: {
          required: true,
          content: {
            "application/json": {
              schema: {
                type: "object",
                required: ["email", "password"],
                properties: {
                  email: { type: "string", format: "email", example: "user@example.com" },
                  password: { type: "string", minLength: 6, example: "password123" },
                  orgName: {
                    type: "string",
                    description: "Defaults to email username if omitted",
                    example: "MyCorp",
                  },
                },
              },
            },
          },
        },
        responses: {
          "201": {
            description: "User and org created",
            content: {
              "application/json": {
                schema: {
                  type: "object",
                  properties: {
                    accessToken: { type: "string", description: "JWT access token (15 min)" },
                    refreshToken: { type: "string", description: "JWT refresh token (7 days)" },
                    user: {
                      type: "object",
                      properties: {
                        id: { type: "string", format: "uuid" },
                        email: { type: "string" },
                        orgId: { type: "string", format: "uuid" },
                      },
                    },
                  },
                },
              },
            },
          },
          "400": { description: "Missing email or password" },
          "409": { description: "Email already exists" },
        },
      },
    },
    "/auth/login": {
      post: {
        tags: ["Auth"],
        summary: "Login with email and password",
        requestBody: {
          required: true,
          content: {
            "application/json": {
              schema: {
                type: "object",
                required: ["email", "password"],
                properties: {
                  email: { type: "string", format: "email" },
                  password: { type: "string" },
                },
              },
            },
          },
        },
        responses: {
          "200": {
            description: "Login successful",
            content: {
              "application/json": {
                schema: {
                  type: "object",
                  properties: {
                    accessToken: { type: "string" },
                    refreshToken: { type: "string" },
                    user: {
                      type: "object",
                      properties: {
                        id: { type: "string", format: "uuid" },
                        email: { type: "string" },
                        orgId: { type: "string", format: "uuid" },
                      },
                    },
                  },
                },
              },
            },
          },
          "401": { description: "Invalid credentials" },
        },
      },
    },
    "/auth/refresh": {
      post: {
        tags: ["Auth"],
        summary: "Refresh access token",
        requestBody: {
          required: true,
          content: {
            "application/json": {
              schema: {
                type: "object",
                required: ["refreshToken"],
                properties: { refreshToken: { type: "string" } },
              },
            },
          },
        },
        responses: {
          "200": {
            description: "New access token",
            content: {
              "application/json": {
                schema: {
                  type: "object",
                  properties: { accessToken: { type: "string" } },
                },
              },
            },
          },
          "401": { description: "Invalid or revoked token" },
        },
      },
    },
    "/auth/me": {
      get: {
        tags: ["Auth"],
        summary: "Get current user profile",
        security: [{ JWT: [] }],
        responses: {
          "200": {
            description: "User profile",
            content: {
              "application/json": {
                schema: {
                  type: "object",
                  properties: {
                    id: { type: "string", format: "uuid" },
                    org_id: { type: "string", format: "uuid" },
                    email: { type: "string" },
                  },
                },
              },
            },
          },
          "401": { description: "Unauthorized" },
        },
      },
    },
    "/auth/logout": {
      post: {
        tags: ["Auth"],
        summary: "Logout and revoke refresh token",
        requestBody: {
          content: {
            "application/json": {
              schema: {
                type: "object",
                properties: { refreshToken: { type: "string" } },
              },
            },
          },
        },
        responses: { "200": { description: "Logged out" } },
      },
    },
    "/track": {
      post: {
        tags: ["Events"],
        summary: "Track an analytics event",
        description: "Accepts an event, deduplicates, and publishes to the processing pipeline. Returns 202.",
        security: [{ ApiKey: [] }],
        requestBody: {
          required: true,
          content: {
            "application/json": {
              schema: {
                type: "object",
                required: ["event", "projectId", "timestamp"],
                properties: {
                  event: { type: "string", example: "page_view" },
                  projectId: { type: "string", format: "uuid" },
                  timestamp: {
                    type: "integer",
                    description: "Epoch timestamp in milliseconds",
                    example: 1748890800000,
                  },
                  userId: { type: "string", example: "user_abc123" },
                  sessionId: { type: "string", example: "session_xyz" },
                  properties: {
                    type: "object",
                    additionalProperties: true,
                    example: { page: "/home", referrer: "https://google.com" },
                  },
                },
              },
            },
          },
        },
        responses: {
          "202": {
            description: "Event accepted or duplicate",
            content: {
              "application/json": {
                schema: {
                  type: "object",
                  properties: {
                    status: {
                      type: "string",
                      enum: ["accepted", "duplicate"],
                    },
                    traceId: {
                      type: "string",
                      format: "uuid",
                      description: "Present only when status is 'accepted'",
                    },
                  },
                },
              },
            },
          },
          "400": { description: "Invalid payload" },
          "401": { description: "Missing or invalid API key" },
          "429": { description: "Rate limit exceeded" },
        },
      },
    },
    "/projects": {
      post: {
        tags: ["Projects"],
        summary: "Create a new project",
        security: [{ JWT: [] }],
        requestBody: {
          required: true,
          content: {
            "application/json": {
              schema: {
                type: "object",
                required: ["name", "orgId"],
                properties: {
                  name: { type: "string", example: "Production" },
                  orgId: { type: "string", format: "uuid" },
                },
              },
            },
          },
        },
        responses: {
          "201": {
            description: "Project created",
            content: {
              "application/json": {
                schema: { $ref: "#/components/schemas/Project" },
              },
            },
          },
        },
      },
      get: {
        tags: ["Projects"],
        summary: "List projects",
        security: [{ JWT: [] }],
        parameters: [
          {
            name: "orgId",
            in: "query",
            required: false,
            schema: { type: "string", format: "uuid" },
            description: "Filter by organization",
          },
        ],
        responses: {
          "200": {
            description: "List of projects",
            content: {
              "application/json": {
                schema: { type: "array", items: { $ref: "#/components/schemas/Project" } },
              },
            },
          },
        },
      },
    },
    "/projects/{id}": {
      get: {
        tags: ["Projects"],
        summary: "Get project by ID",
        security: [{ JWT: [] }],
        parameters: [
          {
            name: "id",
            in: "path",
            required: true,
            schema: { type: "string", format: "uuid" },
          },
        ],
        responses: {
          "200": {
            description: "Project details",
            content: {
              "application/json": {
                schema: { $ref: "#/components/schemas/Project" },
              },
            },
          },
          "404": { description: "Not found" },
        },
      },
    },
    "/projects/{id}/keys": {
      post: {
        tags: ["Projects"],
        summary: "Create an API key for a project",
        description: "Returns the raw API key once. Store it securely — it won't be shown again.",
        security: [{ JWT: [] }],
        parameters: [
          {
            name: "id",
            in: "path",
            required: true,
            schema: { type: "string", format: "uuid" },
          },
        ],
        requestBody: {
          content: {
            "application/json": {
              schema: {
                type: "object",
                properties: {
                  name: { type: "string", default: "default", example: "production-key" },
                },
              },
            },
          },
        },
        responses: {
          "201": {
            description: "API key created",
            content: {
              "application/json": {
                schema: {
                  type: "object",
                  properties: {
                    key: {
                      type: "string",
                      description: "Raw API key (starts with pk_live_)",
                      example: "pk_live_abc123...",
                    },
                    id: { type: "string", format: "uuid" },
                    name: { type: "string" },
                    createdAt: { type: "string", format: "date-time" },
                  },
                },
              },
            },
          },
        },
      },
      get: {
        tags: ["Projects"],
        summary: "List API keys for a project",
        security: [{ JWT: [] }],
        parameters: [
          {
            name: "id",
            in: "path",
            required: true,
            schema: { type: "string", format: "uuid" },
          },
        ],
        responses: {
          "200": {
            description: "List of API keys (excludes secret hashes)",
            content: {
              "application/json": {
                schema: {
                  type: "array",
                  items: {
                    type: "object",
                    properties: {
                      id: { type: "string", format: "uuid" },
                      name: { type: "string" },
                      created_at: { type: "string", format: "date-time" },
                      expires_at: {
                        type: "string",
                        format: "date-time",
                        nullable: true,
                      },
                    },
                  },
                },
              },
            },
          },
        },
      },
    },
    "/projects/{id}/schemas": {
      post: {
        tags: ["Projects"],
        summary: "Create or update an event schema",
        security: [{ JWT: [] }],
        parameters: [
          {
            name: "id",
            in: "path",
            required: true,
            schema: { type: "string", format: "uuid" },
          },
        ],
        requestBody: {
          required: true,
          content: {
            "application/json": {
              schema: {
                type: "object",
                required: ["eventName", "schema"],
                properties: {
                  eventName: { type: "string", example: "purchase" },
                  schema: {
                    type: "object",
                    description: "Zod-compatible JSON schema for validation",
                    example: {
                      type: "object",
                      properties: {
                        amount: { type: "number" },
                        currency: { type: "string" },
                      },
                      required: ["amount"],
                    },
                  },
                },
              },
            },
          },
        },
        responses: {
          "201": {
            description: "Schema created or updated",
            content: {
              "application/json": {
                schema: { $ref: "#/components/schemas/EventSchema" },
              },
            },
          },
        },
      },
      get: {
        tags: ["Projects"],
        summary: "List event schemas for a project",
        security: [{ JWT: [] }],
        parameters: [
          {
            name: "id",
            in: "path",
            required: true,
            schema: { type: "string", format: "uuid" },
          },
        ],
        responses: {
          "200": {
            description: "List of event schemas",
            content: {
              "application/json": {
                schema: {
                  type: "array",
                  items: { $ref: "#/components/schemas/EventSchema" },
                },
              },
            },
          },
        },
      },
    },
    "/projects/{id}/schemas/{eventName}": {
      get: {
        tags: ["Projects"],
        summary: "Get a specific event schema",
        security: [{ JWT: [] }],
        parameters: [
          {
            name: "id",
            in: "path",
            required: true,
            schema: { type: "string", format: "uuid" },
          },
          {
            name: "eventName",
            in: "path",
            required: true,
            schema: { type: "string" },
          },
        ],
        responses: {
          "200": {
            description: "Event schema",
            content: {
              "application/json": {
                schema: { $ref: "#/components/schemas/EventSchema" },
              },
            },
          },
          "404": { description: "Not found" },
        },
      },
    },
    "/api/projects/{id}/metrics": {
      get: {
        tags: ["Query"],
        summary: "Query event metrics over time",
        description: "Returns time-bucketed event counts from TimescaleDB rollups.",
        security: [{ JWT: [] }],
        parameters: [
          { name: "id", in: "path", required: true, schema: { type: "string", format: "uuid" } },
          {
            name: "event",
            in: "query",
            required: true,
            schema: { type: "string" },
            description: "Event name to query",
          },
          {
            name: "from",
            in: "query",
            required: true,
            schema: { type: "string", format: "date-time" },
            description: "Start of time range (ISO 8601)",
          },
          {
            name: "to",
            in: "query",
            required: true,
            schema: { type: "string", format: "date-time" },
            description: "End of time range (ISO 8601)",
          },
          {
            name: "granularity",
            in: "query",
            required: false,
            schema: { type: "string", enum: ["minute", "hour", "day"] },
            description: "Bucket granularity (default: hour)",
          },
        ],
        responses: {
          "200": {
            description: "Time-series metric data",
            content: {
              "application/json": {
                schema: {
                  type: "array",
                  items: {
                    type: "object",
                    properties: {
                      bucket: { type: "string", format: "date-time" },
                      count: { type: "integer" },
                      unique_users: { type: "integer" },
                    },
                  },
                },
              },
            },
          },
          "400": { description: "Missing required parameters" },
        },
      },
    },
    "/api/projects/{id}/funnels": {
      get: {
        tags: ["Query"],
        summary: "Run funnel analysis",
        description: "Calculates conversion between event steps within a time window using ClickHouse.",
        security: [{ JWT: [] }],
        parameters: [
          { name: "id", in: "path", required: true, schema: { type: "string", format: "uuid" } },
          {
            name: "steps",
            in: "query",
            required: true,
            schema: { type: "string" },
            description: "JSON array of event names, e.g. `[\"page_view\",\"signup\",\"purchase\"]`",
            example: '["page_view","signup","purchase"]',
          },
          {
            name: "window",
            in: "query",
            required: false,
            schema: { type: "string", default: "7d" },
            description: "Conversion window (e.g. 30m, 1h, 7d)",
            example: "7d",
          },
          {
            name: "from",
            in: "query",
            required: false,
            schema: { type: "string", format: "date-time" },
          },
          {
            name: "to",
            in: "query",
            required: false,
            schema: { type: "string", format: "date-time" },
          },
        ],
        responses: {
          "200": {
            description: "Step-to-step conversion counts",
            content: {
              "application/json": {
                schema: {
                  type: "array",
                  items: {
                    type: "object",
                    properties: {
                      from: { type: "string" },
                      to: { type: "string" },
                      users: { type: "integer" },
                    },
                  },
                },
              },
            },
          },
          "400": { description: "Missing or invalid parameters" },
          "503": { description: "Service degraded (circuit breaker open)" },
        },
      },
    },
    "/api/projects/{id}/retention": {
      get: {
        tags: ["Query"],
        summary: "Run retention analysis",
        description: "Weekly cohort retention analysis using ClickHouse.",
        security: [{ JWT: [] }],
        parameters: [
          { name: "id", in: "path", required: true, schema: { type: "string", format: "uuid" } },
          {
            name: "cohortEvent",
            in: "query",
            required: true,
            schema: { type: "string" },
            description: "Event that defines the cohort",
          },
          {
            name: "returnEvent",
            in: "query",
            required: true,
            schema: { type: "string" },
            description: "Event that counts as a return",
          },
          {
            name: "periods",
            in: "query",
            required: false,
            schema: { type: "integer", default: 8 },
            description: "Number of weekly periods",
          },
          {
            name: "from",
            in: "query",
            required: false,
            schema: { type: "string", format: "date-time" },
          },
          {
            name: "to",
            in: "query",
            required: false,
            schema: { type: "string", format: "date-time" },
          },
        ],
        responses: {
          "200": {
            description: "Cohort retention data",
            content: {
              "application/json": {
                schema: {
                  type: "array",
                  items: {
                    type: "object",
                    properties: {
                      cohort: { type: "string", format: "date" },
                      total: { type: "integer" },
                    },
                    additionalProperties: {
                      type: "integer",
                      description: "period_{N} columns (period_0 through period_{N-1})",
                    },
                  },
                },
              },
            },
          },
          "400": { description: "Missing required parameters" },
          "503": { description: "Service degraded (circuit breaker open)" },
        },
      },
    },
    "/api/projects/{id}/users": {
      get: {
        tags: ["Query"],
        summary: "Query distinct users",
        description: "Paginated list of users who triggered events, with optional filtering.",
        security: [{ JWT: [] }],
        parameters: [
          { name: "id", in: "path", required: true, schema: { type: "string", format: "uuid" } },
          {
            name: "filter",
            in: "query",
            required: false,
            schema: { type: "string" },
            description: 'Field filter in `field:value` format (e.g. `country:US`, `browser:Chrome`)',
          },
          {
            name: "limit",
            in: "query",
            required: false,
            schema: { type: "integer", default: 50, maximum: 1000 },
          },
          {
            name: "offset",
            in: "query",
            required: false,
            schema: { type: "integer", default: 0 },
          },
        ],
        responses: {
          "200": {
            description: "Paginated user list",
            content: {
              "application/json": {
                schema: {
                  type: "array",
                  items: {
                    type: "object",
                    properties: {
                      user_id: { type: "string" },
                      country: { type: "string" },
                      city: { type: "string" },
                      browser: { type: "string" },
                      os: { type: "string" },
                      device_type: { type: "string" },
                      first_seen: { type: "string", format: "date-time" },
                      last_seen: { type: "string", format: "date-time" },
                    },
                  },
                },
              },
            },
          },
          "503": { description: "Service degraded (circuit breaker open)" },
        },
      },
    },
    "/api/projects/{id}/events/live": {
      get: {
        tags: ["Query"],
        summary: "Get live event counters",
        description: "Current minute event counts from Redis counters.",
        security: [{ JWT: [] }],
        parameters: [
          { name: "id", in: "path", required: true, schema: { type: "string", format: "uuid" } },
        ],
        responses: {
          "200": {
            description: "Live event counts",
            content: {
              "application/json": {
                schema: {
                  type: "object",
                  properties: {
                    timestamp: {
                      type: "integer",
                      description: "Current minute bucket (epoch ms)",
                    },
                    events: {
                      type: "array",
                      items: {
                        type: "object",
                        properties: {
                          event: { type: "string" },
                          count: { type: "integer" },
                        },
                      },
                    },
                  },
                },
              },
            },
          },
        },
      },
    },
    "/admin/dlq": {
      get: {
        tags: ["Admin"],
        summary: "List dead-letter queue events",
        security: [{ JWT: [] }],
        parameters: [
          {
            name: "status",
            in: "query",
            required: false,
            schema: {
              type: "string",
              enum: ["pending", "retrying", "retried", "discarded"],
              default: "pending",
            },
          },
          {
            name: "limit",
            in: "query",
            required: false,
            schema: { type: "integer", default: 50, maximum: 500 },
          },
          {
            name: "offset",
            in: "query",
            required: false,
            schema: { type: "integer", default: 0 },
          },
        ],
        responses: {
          "200": {
            description: "Paginated DLQ events",
            content: {
              "application/json": {
                schema: {
                  type: "object",
                  properties: {
                    total: { type: "integer" },
                    events: {
                      type: "array",
                      items: { $ref: "#/components/schemas/DLQEvent" },
                    },
                  },
                },
              },
            },
          },
        },
      },
    },
    "/admin/dlq/{id}": {
      get: {
        tags: ["Admin"],
        summary: "Get a single DLQ event",
        security: [{ JWT: [] }],
        parameters: [
          {
            name: "id",
            in: "path",
            required: true,
            schema: { type: "integer", format: "int64" },
          },
        ],
        responses: {
          "200": {
            description: "DLQ event details",
            content: {
              "application/json": {
                schema: { $ref: "#/components/schemas/DLQEventDetail" },
              },
            },
          },
          "404": { description: "Not found" },
        },
      },
    },
    "/admin/dlq/{id}/retry": {
      post: {
        tags: ["Admin"],
        summary: "Retry a DLQ event",
        description: "Re-publishes the original message to its topic.",
        security: [{ JWT: [] }],
        parameters: [
          {
            name: "id",
            in: "path",
            required: true,
            schema: { type: "integer", format: "int64" },
          },
        ],
        responses: {
          "200": {
            description: "Event sent for retry",
            content: {
              "application/json": {
                schema: {
                  type: "object",
                  properties: {
                    ok: { type: "boolean" },
                    topic: { type: "string" },
                    status: { type: "string" },
                  },
                },
              },
            },
          },
          "404": { description: "Event not found or not in pending state" },
        },
      },
    },
    "/admin/dlq/{id}/discard": {
      post: {
        tags: ["Admin"],
        summary: "Discard a DLQ event",
        description: "Marks the event as discarded — it won't be retried.",
        security: [{ JWT: [] }],
        parameters: [
          {
            name: "id",
            in: "path",
            required: true,
            schema: { type: "integer", format: "int64" },
          },
        ],
        responses: {
          "200": {
            description: "Event discarded",
            content: {
              "application/json": {
                schema: {
                  type: "object",
                  properties: {
                    ok: { type: "boolean" },
                    id: { type: "integer" },
                  },
                },
              },
            },
          },
          "404": { description: "Not found or not pending" },
        },
      },
    },
    "/health": {
      get: {
        tags: ["System"],
        summary: "Health check",
        responses: {
          "200": {
            description: "Service is healthy",
            content: {
              "application/json": {
                schema: {
                  type: "object",
                  properties: { status: { type: "string", example: "ok" } },
                },
              },
            },
          },
        },
      },
    },
    "/live": {
      get: {
        tags: ["WebSocket"],
        summary: "WebSocket for live event updates",
        description:
          "Upgrades to a WebSocket connection. Subscribes to real-time event counters for the given project. Requires JWT as a query parameter.\n\nScalar does not support WebSocket preview. Use a WebSocket client like `wscat`:\n```bash\nwscat -c \"ws://localhost:3000/live?token=<jwt>&projectId=<projectId>\"\n```",
        parameters: [
          {
            name: "token",
            in: "query",
            required: true,
            schema: { type: "string" },
            description: "JWT access token",
          },
          {
            name: "projectId",
            in: "query",
            required: true,
            schema: { type: "string", format: "uuid" },
          },
        ],
        responses: {
          "101": { description: "WebSocket upgrade successful" },
          "400": { description: "Missing token or projectId" },
          "401": { description: "Invalid token" },
        },
      },
    },
  },
  components: {
    securitySchemes: {
      ApiKey: {
        type: "http",
        scheme: "bearer",
        description: "API key starting with pk_live_",
      },
      JWT: {
        type: "http",
        scheme: "bearer",
        description: "JWT access token obtained from /auth/login",
        bearerFormat: "JWT",
      },
    },
    schemas: {
      Project: {
        type: "object",
        properties: {
          id: { type: "string", format: "uuid" },
          org_id: { type: "string", format: "uuid" },
          name: { type: "string" },
          created_at: { type: "string", format: "date-time" },
          updated_at: { type: "string", format: "date-time" },
        },
      },
      EventSchema: {
        type: "object",
        properties: {
          id: { type: "string", format: "uuid" },
          project_id: { type: "string", format: "uuid" },
          event_name: { type: "string" },
          schema: { type: "object", description: "JSON schema for event validation" },
          version: { type: "integer" },
          created_at: { type: "string", format: "date-time" },
          updated_at: { type: "string", format: "date-time" },
        },
      },
      DLQEvent: {
        type: "object",
        properties: {
          id: { type: "integer" },
          original_topic: { type: "string" },
          original_partition: { type: "integer" },
          original_offset: { type: "string" },
          original_key: { type: "string", nullable: true },
          reason: { type: "string" },
          status: {
            type: "string",
            enum: ["pending", "retrying", "retried", "discarded"],
          },
          retry_count: { type: "integer" },
          created_at: { type: "string", format: "date-time" },
          updated_at: { type: "string", format: "date-time" },
        },
      },
      DLQEventDetail: {
        allOf: [
          { $ref: "#/components/schemas/DLQEvent" },
          {
            type: "object",
            properties: {
              original_value: { type: "string" },
              original_headers: {
                type: "object",
                additionalProperties: { type: "string" },
              },
              last_error: { type: "string", nullable: true },
            },
          },
        ],
      },
    },
  },
} as const;

export function createOpenApiRoutes<E extends Env>(app: Hono<E>) {
  app.get("/openapi.json", (c: Context) => {
    return c.json(spec);
  });

  app.get(
    "/docs",
    apiReference({
      spec: { url: "/openapi.json" },
      pageTitle: "Catalyst API Reference",
    }),
  );
}
