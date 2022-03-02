import { z } from "zod";

const FeastSavedDatasetSchema = z.object({
  spec: z.object({
    name: z.string(),
    features: z.array(z.string()),
    joinKeys: z.array(z.string()),
    storage: z.object({
      fileStorage: z.object({
        fileFormat: z.object({
          parquestFormat: z.object({}).optional(),
        }),
        fileUrl: z.string(),
      }),
    }),
    featureService: z
      .object({
        spec: z.object({
          name: z.string(),
        }),
      })
      .transform((obj) => {
        return obj.spec.name;
      }),
    profile: z.string().optional(),
  }),
  meta: z.object({
    createdTimestamp: z.string().transform((val) => new Date(val)),
    minEventTimestamp: z.string().transform((val) => new Date(val)),
    maxEventTimestamp: z.string().transform((val) => new Date(val)),
  }),
});

type FeastSavedDatasetType = z.infer<typeof FeastSavedDatasetSchema>;

export { FeastSavedDatasetSchema };
export type { FeastSavedDatasetType };
