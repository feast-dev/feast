import { z } from "zod";
import { FeastFeatureColumnSchema } from "./feastFeatureViews";
import {FeastDatasourceSchema} from "./feastDatasources";

const FeatureViewProjectionSchema = z.object({
  featureViewProjection: z.object({
    featureViewName: z.string(),
    featureColumns: z.array(FeastFeatureColumnSchema),
  }),
});

const StreamSourceSchema = z.object({
    type: z.string(),
    name: z.string(),
    owner: z.string().optional(),
    description: z.string().optional(),
});

const FeastSFVSchema = z.object({
  spec: z.object({
    name: z.string(),
    features: z.array(FeastFeatureColumnSchema),
    batchSource: FeastDatasourceSchema,
    streamSource: StreamSourceSchema,
    userDefinedFunction: z.object({
      name: z.string(),
      body: z.string(),
    }),
  }),
  meta: z.object({
    createdTimestamp: z.string().transform((val) => new Date(val)),
    lastUpdatedTimestamp: z.string().transform((val) => new Date(val)),
  }),
});

type FeastSFVType = z.infer<typeof FeastSFVSchema>;
type StreamSourceType = z.infer<typeof StreamSourceSchema>;
type FeatureViewProjectionType = z.infer<typeof FeatureViewProjectionSchema>;

export { FeastSFVSchema };
export type { FeastSFVType, StreamSourceType, FeatureViewProjectionType};
