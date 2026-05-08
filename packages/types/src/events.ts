import { z } from "zod";

export const RawEventSchema = z.object({
  event: z.string(),
  projectId: z.string(),
  properties: z.record(z.string(), z.unknown()).optional(),
  timestamp: z.number(),
  userId: z.string().optional(),
});

export type RawEvent = z.infer<typeof RawEventSchema>;

export const ValidatedEventSchema = RawEventSchema.extend({
  traceId: z.string().optional(),
  validatedAt: z.number(),
});

export type ValidatedEvent = z.infer<typeof ValidatedEventSchema>;

export const EnrichedEventSchema = ValidatedEventSchema.extend({
  browser: z.string().optional(),
  city: z.string().optional(),
  country: z.string().optional(),
  deviceType: z.string().optional(),
  enrichedAt: z.number(),
  os: z.string().optional(),
  sessionId: z.string().optional(),
});

export type EnrichedEvent = z.infer<typeof EnrichedEventSchema>;
