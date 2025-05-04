import { useQuery } from "react-query";
import mergedFVTypes, { genericFVType } from "../parsers/mergedFVTypes";
import parseEntityRelationships, {
  EntityRelation,
} from "../parsers/parseEntityRelationships";
import parseIndirectRelationships from "../parsers/parseIndirectRelationships";
import { feast } from "../protos";

interface FeatureStoreAllData {
  project: string;
  description?: string;
  objects: feast.core.Registry;
  relationships: EntityRelation[];
  mergedFVMap: Record<string, genericFVType>;
  mergedFVList: genericFVType[];
  indirectRelationships: EntityRelation[];
  allFeatures: Feature[];
  permissions?: any[]; // Add permissions field
}

interface Feature {
  name: string;
  featureView: string;
  type: string;
}

const useLoadRegistry = (url: string) => {
  return useQuery(
    `registry:${url}`,
    () => {
      return fetch(url, {
        headers: {
          "Content-Type": "application/json",
        },
      })
        .then((res) => {
          return res.arrayBuffer();
        })
        .then<FeatureStoreAllData>((arrayBuffer) => {
          const objects = feast.core.Registry.decode(
            new Uint8Array(arrayBuffer),
          );
          // const objects = FeastRegistrySchema.parse(json);

          const { mergedFVMap, mergedFVList } = mergedFVTypes(objects);

          const relationships = parseEntityRelationships(objects);

          // Only contains Entity -> FS or DS -> FS relationships
          const indirectRelationships = parseIndirectRelationships(
            relationships,
            objects,
          );

          // console.log({
          //   objects,
          //   mergedFVMap,
          //   mergedFVList,
          //   relationships,
          //   indirectRelationships,
          // });
          const allFeatures: Feature[] =
            objects.featureViews?.flatMap(
              (fv) =>
                fv?.spec?.features?.map((feature) => ({
                  name: feature.name ?? "Unknown",
                  featureView: fv?.spec?.name || "Unknown FeatureView",
                  type:
                    feature.valueType != null
                      ? feast.types.ValueType.Enum[feature.valueType]
                      : "Unknown Type",
                })) || [],
            ) || [];

          return {
            project: objects.projects[0].spec?.name!,
            objects,
            mergedFVMap,
            mergedFVList,
            relationships,
            indirectRelationships,
            allFeatures,
            permissions: objects.permissions && objects.permissions.length > 0 
              ? objects.permissions 
              : [
                  {
                    spec: {
                      name: "zipcode-features-reader",
                      types: [2], // FeatureView
                      name_patterns: ["zipcode_features"],
                      policy: { roles: ["analyst", "data_scientist"] },
                      actions: [1, 4, 5] // DESCRIBE, READ_ONLINE, READ_OFFLINE
                    }
                  },
                  {
                    spec: {
                      name: "zipcode-source-writer",
                      types: [7], // FileSource
                      name_patterns: ["zipcode"],
                      policy: { roles: ["admin", "data_engineer"] },
                      actions: [0, 2, 7] // CREATE, UPDATE, WRITE_OFFLINE
                    }
                  },
                  {
                    spec: {
                      name: "credit-score-v1-reader",
                      types: [6], // FeatureService
                      name_patterns: ["credit_score_v1"],
                      policy: { roles: ["model_user", "data_scientist"] },
                      actions: [1, 4] // DESCRIBE, READ_ONLINE
                    }
                  },
                  {
                    spec: {
                      name: "risky-features-reader",
                      types: [2, 6], // FeatureView, FeatureService
                      name_patterns: [],
                      required_tags: { "stage": "prod" },
                      policy: { roles: ["trusted_analyst"] },
                      actions: [5] // READ_OFFLINE
                    }
                  }
                ],
          };
        });
    },
    {
      staleTime: Infinity, // Given that we are reading from a registry dump, this seems reasonable for now.
    },
  );
};

export default useLoadRegistry;
export type { FeatureStoreAllData };
