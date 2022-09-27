import { z } from "zod";
import { FeastFeatureColumnSchema } from "./feastFeatureViews";

const FeatureViewProjectionSchema = z.object({
  featureViewProjection: z.object({
    featureViewName: z.string(),
    featureColumns: z.array(FeastFeatureColumnSchema),
  }),
});

const StreamSourceSchema = z.object({
  streamSource: z.object({
    type: z.string(),
    name: z.string(),
    streamDataOptions: z.object({
      schema: z.array(FeastFeatureColumnSchema),
    }),
  }),
});

const SFVInputsSchema = z.union([
  FeatureViewProjectionSchema,
  StreamSourceSchema,
]);

const FeastSFVSchema = z.object({
  spec: z.object({
    name: z.string(),
    features: z.array(FeastFeatureColumnSchema),
    sources: z.record(SFVInputsSchema),
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

export { FeastSFVSchema };
export type { FeastSFVType, StreamSourceType};
