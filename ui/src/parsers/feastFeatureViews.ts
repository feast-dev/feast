import { z } from "zod";
import { FEAST_FEATURE_VALUE_TYPES } from "./types";

const FeastFeatureColumnSchema = z.object({
  name: z.string(),
  valueType: z.nativeEnum(FEAST_FEATURE_VALUE_TYPES),
});

const FeastBatchSourceSchema = z.object({
  type: z.string(),
  eventTimestampColumn: z.string().optional(),
  createdTimestampColumn: z.string().optional(),
  fileOptions: z.object({
    fileUrl: z.string().optional(),
  }).optional(),
  name: z.string().optional(),
  meta: z.object({
    earliestEventTimestamp: z.string().transform((val) => new Date(val)),
    latestEventTimestamp: z.string().transform((val) => new Date(val)),
  }).optional(),
  requestDataOptions: z.object({
    schema: z.record(z.nativeEnum(FEAST_FEATURE_VALUE_TYPES)),
  }).optional(),
  bigqueryOptions: z.object({
    tableRef: z.string().optional(),
    dbtModelSerialized: z.string().optional()
  }).optional(),
  dataSourceClassType: z.string(),
});

const FeastFeatureViewSchema = z.object({
  spec: z.object({
    name: z.string(),
    entities: z.array(z.string()),
    features: z.array(FeastFeatureColumnSchema),
    ttl: z.string().transform((val) => parseInt(val)),
    batchSource: FeastBatchSourceSchema,
    online: z.boolean(),
    tags: z.record(z.string()).optional(),
  }),
  meta: z.object({
    createdTimestamp: z.string().transform((val) => new Date(val)).optional(),
    lastUpdatedTimestamp: z.string().transform((val) => new Date(val)).optional(),
    materializationIntervals: z
      .array(
        z.object({
          startTime: z.string().transform((val) => new Date(val)),
          endTime: z.string().transform((val) => new Date(val)),
        })
      )
      .optional(),
  }),
});

type FeastFeatureViewType = z.infer<typeof FeastFeatureViewSchema>;
type FeastFeatureColumnType = z.infer<typeof FeastFeatureColumnSchema>;

export { FeastFeatureViewSchema, FeastFeatureColumnSchema };
export type { FeastFeatureViewType, FeastFeatureColumnType };
