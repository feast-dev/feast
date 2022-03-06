import { z } from "zod";
import { FeastFeatureColumnSchema } from "./feastFeatureViews";
import { FEAST_FEATURE_VALUE_TYPES } from "./types";

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
      schema: z.record(z.nativeEnum(FEAST_FEATURE_VALUE_TYPES)),
    }),
  }),
});

const ODFVInputsSchema = z.union([
  FeatureViewProjectionSchema,
  RequestDataSourceSchema,
]);

const FeastODFVSchema = z.object({
  spec: z.object({
    name: z.string(),
    features: z.array(FeastFeatureColumnSchema),
    inputs: z.record(ODFVInputsSchema),
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

type FeastODFVType = z.infer<typeof FeastODFVSchema>;
type RequestDataSourceType = z.infer<typeof RequestDataSourceSchema>;
type FeatureViewProjectionType = z.infer<typeof FeatureViewProjectionSchema>;

export { FeastODFVSchema };
export type { FeastODFVType, RequestDataSourceType, FeatureViewProjectionType };
