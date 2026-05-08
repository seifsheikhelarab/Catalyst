import { z } from 'zod';

export const ProjectSchema = z.object({
  id: z.string(),
  name: z.string(),
  apiKeyHash: z.string().optional(),
  createdAt: z.number(),
  updatedAt: z.number(),
});

export type Project = z.infer<typeof ProjectSchema>;

export const UserSchema = z.object({
  id: z.string(),
  email: z.string().email(),
  orgId: z.string(),
  createdAt: z.number(),
});

export type User = z.infer<typeof UserSchema>;

export const ApiKeySchema = z.object({
  key: z.string().startsWith('pk_live_'),
  projectId: z.string(),
  createdAt: z.number(),
  expiresAt: z.number().optional(),
});

export type ApiKey = z.infer<typeof ApiKeySchema>;

export const EventSchemaDefinitionSchema = z.object({
  projectId: z.string(),
  version: z.number(),
  schema: z.record(z.any()),
  createdAt: z.number(),
});

export type EventSchemaDefinition = z.infer<typeof EventSchemaDefinitionSchema>;