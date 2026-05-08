import { z } from 'zod';

export const RawEventSchema = z.object({
  projectId: z.string(),
  event: z.string(),
  userId: z.string().optional(),
  timestamp: z.number(),
  properties: z.record(z.unknown()).optional(),
});

export type RawEvent = z.infer<typeof RawEventSchema>;

export const ValidatedEventSchema = RawEventSchema.extend({
  validatedAt: z.number(),
  traceId: z.string().optional(),
});

export type ValidatedEvent = z.infer<typeof ValidatedEventSchema>;

export const EnrichedEventSchema = ValidatedEventSchema.extend({
  country: z.string().optional(),
  city: z.string().optional(),
  deviceType: z.string().optional(),
  browser: z.string().optional(),
  os: z.string().optional(),
  sessionId: z.string().optional(),
  enrichedAt: z.number(),
});

export type EnrichedEvent = z.infer<typeof EnrichedEventSchema>;