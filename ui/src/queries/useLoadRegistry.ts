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

          const objects = feast.core.Registry.decode(new Uint8Array(arrayBuffer));
          // const objects = FeastRegistrySchema.parse(json);

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
            project: objects.projects[0].spec?.name!,
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
