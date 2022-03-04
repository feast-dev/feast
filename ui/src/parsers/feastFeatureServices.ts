import { z } from "zod";
import { FEAST_FEATURE_VALUE_TYPES } from "./types";

const FeatureColumnInService = z.object({
  name: z.string(),
  valueType: z.nativeEnum(FEAST_FEATURE_VALUE_TYPES),
});

const FeatureInServiceSchema = z.object({
  featureViewName: z.string(),
  featureColumns: z.array(FeatureColumnInService),
});

const FeastFeatureServiceSchema = z.object({
  spec: z.object({
    name: z.string(),
    features: z.array(FeatureInServiceSchema),
    tags: z.record(z.string()).optional(),
    description: z.string().optional(),
  }),
  meta: z.object({
    createdTimestamp: z.string().transform((val) => new Date(val)),
  }),
});

type FeastFeatureServiceType = z.infer<typeof FeastFeatureServiceSchema>;
type FeastFeatureInServiceType = z.infer<typeof FeatureInServiceSchema>;

export { FeastFeatureServiceSchema };
export type { FeastFeatureServiceType, FeastFeatureInServiceType };
