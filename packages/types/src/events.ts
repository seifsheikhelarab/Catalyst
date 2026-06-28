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

export const DeadLetterEventSchema = z.object({
  error: z.string(),
  originalEvent: RawEventSchema,
  reason: z.string(),
  timestamp: z.number(),
});

export type DeadLetterEvent = z.infer<typeof DeadLetterEventSchema>;

export const DLQEnvelopeSchema = z.object({
  originalTopic: z.string(),
  originalPartition: z.number(),
  originalOffset: z.string(),
  originalKey: z.string().optional(),
  originalValue: z.string().optional(),
  originalHeaders: z.record(z.string(), z.string()).optional(),
  reason: z.string(),
  timestamp: z.number(),
});

export type DLQEnvelope = z.infer<typeof DLQEnvelopeSchema>;
