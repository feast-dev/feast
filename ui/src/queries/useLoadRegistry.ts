import { useQuery } from "react-query";
import {
  FeastRegistrySchema,
  FeastRegistryType,
} from "../parsers/feastRegistry";
import mergedFVTypes, { genericFVType } from "../parsers/mergedFVTypes";
import parseEntityRelationships, {
  EntityRelation,
} from "../parsers/parseEntityRelationships";
import parseIndirectRelationships from "../parsers/parseIndirectRelationships";

interface FeatureStoreAllData {
  project: string;
  description?: string;
  objects: FeastRegistryType;
  relationships: EntityRelation[];
  mergedFVMap: Record<string, genericFVType>;
  mergedFVList: genericFVType[];
  indirectRelationships: EntityRelation[];
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
          return res.json();
        })
        .then<FeatureStoreAllData>((json) => {
          const objects = FeastRegistrySchema.parse(json);

          const { mergedFVMap, mergedFVList } = mergedFVTypes(objects);

          const relationships = parseEntityRelationships(objects);

          // Only contains Entity -> FS or DS -> FS relationships
          const indirectRelationships = parseIndirectRelationships(
            relationships,
            objects
          );

          // console.log({
          //   objects,
          //   mergedFVMap,
          //   mergedFVList,
          //   relationships,
          //   indirectRelationships,
          // });

          return {
            project: objects.project,
            objects,
            mergedFVMap,
            mergedFVList,
            relationships,
            indirectRelationships,
          };
        });
    },
    {
      staleTime: Infinity, // Given that we are reading from a registry dump, this seems reasonable for now.
    }
  );
};

export default useLoadRegistry;
export type { FeatureStoreAllData };
