import { z } from "zod";
import { FEAST_FEATURE_VALUE_TYPES } from "./types";

const FeastEntitySchema = z.object({
  spec: z.object({
    name: z.string(),
    valueType: z.nativeEnum(FEAST_FEATURE_VALUE_TYPES),
    joinKey: z.string(),
    description: z.string().optional(),
    labels: z.record(z.string()).optional(),
  }),
  meta: z.object({
    createdTimestamp: z.string().transform((val) => new Date(val)).optional(),
    lastUpdatedTimestamp: z.string().transform((val) => new Date(val)).optional(),
  }),
});

type FeastEntityType = z.infer<typeof FeastEntitySchema>;

export { FeastEntitySchema };
export type { FeastEntityType };
