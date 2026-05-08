import { z } from "zod";

export const ProjectSchema = z.object({
  apiKeyHash: z.string().optional(),
  createdAt: z.number(),
  id: z.string(),
  name: z.string(),
  updatedAt: z.number(),
});

export type Project = z.infer<typeof ProjectSchema>;

export const UserSchema = z.object({
  createdAt: z.number(),
  email: z.string().email(),
  id: z.string(),
  orgId: z.string(),
});

export type User = z.infer<typeof UserSchema>;

export const ApiKeySchema = z.object({
  createdAt: z.number(),
  expiresAt: z.number().optional(),
  key: z.string().startsWith("pk_live_"),
  projectId: z.string(),
});

export type ApiKey = z.infer<typeof ApiKeySchema>;

export const EventSchemaDefinitionSchema = z.object({
  createdAt: z.number(),
  projectId: z.string(),
  schema: z.record(z.string(), z.unknown()),
  version: z.number(),
});

export type EventSchemaDefinition = z.infer<typeof EventSchemaDefinitionSchema>;
