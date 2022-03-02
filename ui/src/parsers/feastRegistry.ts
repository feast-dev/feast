import { z } from "zod";
import { FeastDatasourceSchema } from "./feastDatasources";
import { FeastEntitySchema } from "./feastEntities";
import { FeastFeatureServiceSchema } from "./feastFeatureServices";
import { FeastFeatureViewSchema } from "./feastFeatureViews";
import { FeastSavedDatasetSchema } from "./feastSavedDataset";
import { FeastODFVSchema } from "./feastODFVS";

const FeastRegistrySchema = z.object({
  project: z.string(),
  dataSources: z.array(FeastDatasourceSchema).optional(),
  entities: z.array(FeastEntitySchema).optional(),
  featureViews: z.array(FeastFeatureViewSchema).optional(),
  onDemandFeatureViews: z.array(FeastODFVSchema).optional(),
  featureServices: z.array(FeastFeatureServiceSchema).optional(),
  savedDatasets: z.array(FeastSavedDatasetSchema).optional(),
});

type FeastRegistryType = z.infer<typeof FeastRegistrySchema>;

export { FeastRegistrySchema };
export type { FeastRegistryType };
