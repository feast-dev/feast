import { EntityRelation } from "./parseEntityRelationships";
import { FEAST_FCO_TYPES } from "./types";
import { feast } from "../protos";

const parseIndirectRelationships = (
  relationships: EntityRelation[],
  objects: feast.core.Registry
) => {
  const indirectLinks: EntityRelation[] = [];

  // Only contains Entity -> FS or DS -> FS relationships
  objects.featureServices?.forEach((featureService) => {
    featureService.spec?.features?.forEach((featureView) => {
      relationships
        .filter(
          (relationship) =>
            relationship.target.name === featureView.featureViewName
        )
        .forEach((relationship) => {
          indirectLinks.push({
            source: relationship.source,
            target: {
              type: FEAST_FCO_TYPES["featureService"],
              name: featureService.spec?.name!,
            },
          });
        });
    });
  });
  return indirectLinks;
};

export default parseIndirectRelationships;
