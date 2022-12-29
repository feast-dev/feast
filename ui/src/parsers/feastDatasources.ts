import { z } from "zod";
import { FeastFeatureColumnSchema } from "./feastFeatureViews";

const FeastDatasourceSchema = z.object({
  type: z.string(),
  eventTimestampColumn: z.string().optional(),
  createdTimestampColumn: z.string().optional(),
  fileOptions: z.object({
    uri: z.string().optional(),
  }).optional(),
  name: z.string(),
  description: z.string().optional(),
  owner: z.string().optional(),
  meta: z.object({
    latestEventTimestamp: z.string().transform((val) => new Date(val)),
    earliestEventTimestamp: z.string().transform((val) => new Date(val)),
  }).optional(),
  requestDataOptions: z.object({
    schema: z.array(FeastFeatureColumnSchema),
  }).optional(),
  bigqueryOptions: z.object({
    tableRef: z.string().optional(),
    dbtModelSerialized: z.string().optional()
  }).optional(),
});

type FeastDatasourceType = z.infer<typeof FeastDatasourceSchema>;

export { FeastDatasourceSchema };
export type { FeastDatasourceType };
