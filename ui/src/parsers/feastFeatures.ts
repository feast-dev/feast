import { z } from "zod";
import { FEAST_FEATURE_VALUE_TYPES } from "./types";
import { jsonSchema } from "./jsonType"

const FeastFeatureSchema = z.object({
    name: z.string(),
    valueType: z.nativeEnum(FEAST_FEATURE_VALUE_TYPES),
    metadata: jsonSchema.optional(),
});

export { FeastFeatureSchema };
