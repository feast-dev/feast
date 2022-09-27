import { z } from "zod";
import { FeastFeatureColumnSchema } from "./feastFeatureViews";

const FeatureViewProjectionSchema = z.object({
  featureViewProjection: z.object({
    featureViewName: z.string(),
    featureColumns: z.array(FeastFeatureColumnSchema),
  }),
});

const RequestDataSourceSchema = z.object({
  requestDataSource: z.object({
    type: z.string(),
    name: z.string(),
    requestDataOptions: z.object({
      schema: z.array(FeastFeatureColumnSchema),
    }),
  }),
});

const StreamDataSourceSchema = z.object({
  streamDataSource: z.object({
    type: z.string(),
    name: z.string(),
    streamDataOptions: z.object({
      schema: z.array(FeastFeatureColumnSchema),
    }),
  }),
});

const ODFVInputsSchema = z.union([
  FeatureViewProjectionSchema,
  StreamDataSourceSchema,
]);

const SFVInputsSchema = z.union([
  FeatureViewProjectionSchema,
  StreamDataSourceSchema,
]);

const FeastODFVSchema = z.object({
  spec: z.object({
    name: z.string(),
    features: z.array(FeastFeatureColumnSchema),
    sources: z.record(ODFVInputsSchema),
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
type StreamDataSourceType = z.infer<typeof StreamDataSourceSchema>;

export { FeastSFVSchema };
export type { FeastSFVType, StreamDataSourceType};
