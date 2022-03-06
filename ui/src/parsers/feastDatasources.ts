import { z } from "zod";
import { FEAST_FEATURE_VALUE_TYPES } from "./types";

const FeastDatasourceSchema = z.object({
  type: z.string(),
  eventTimestampColumn: z.string().optional(),
  createdTimestampColumn: z.string().optional(),
  fileOptions: z.object({
    fileUrl: z.string().optional(),
  }).optional(),
  name: z.string(),
  meta: z.object({
    latestEventTimestamp: z.string().transform((val) => new Date(val)),
    earliestEventTimestamp: z.string().transform((val) => new Date(val)),
  }).optional(),
  requestDataOptions: z.object({
    schema: z.record(z.nativeEnum(FEAST_FEATURE_VALUE_TYPES)),
  }).optional(),
  bigqueryOptions: z.object({
    tableRef: z.string().optional(),
    dbtModelSerialized: z.string().optional()
  }).optional(),
});

type FeastDatasourceType = z.infer<typeof FeastDatasourceSchema>;

export { FeastDatasourceSchema };
export type { FeastDatasourceType };
