import { FEAST_FCO_TYPES } from "./types";
import { feast } from "../protos";

interface EntityReference {
  type: FEAST_FCO_TYPES;
  name: string;
}

interface EntityRelation {
  source: EntityReference;
  target: EntityReference;
}

const parseEntityRelationships = (objects: feast.core.Registry) => {
  const links: EntityRelation[] = [];

  objects.featureServices?.forEach((fs) => {
    fs.spec?.features!.forEach((feature) => {
      links.push({
        source: {
          type: FEAST_FCO_TYPES["featureView"],
          name: feature?.featureViewName!,
        },
        target: {
          type: FEAST_FCO_TYPES["featureService"],
          name: fs.spec?.name!,
        },
      });
    });
  });

  objects.featureViews?.forEach((fv) => {
    fv.spec?.entities?.forEach((ent) => {
      links.push({
        source: {
          type: FEAST_FCO_TYPES["entity"],
          name: ent,
        },
        target: {
          type: FEAST_FCO_TYPES["featureView"],
          name: fv.spec?.name!,
        },
      });
    });
    if (fv.spec?.batchSource) {
      links.push({
        source: {
          type: FEAST_FCO_TYPES["dataSource"],
          name: fv.spec.batchSource.name || "",
        },
        target: {
          type: FEAST_FCO_TYPES["featureView"],
          name: fv.spec?.name!,
        },
      });
    }
  });

  objects.onDemandFeatureViews?.forEach((fv) => {
    // Entity relationships
    fv.spec?.entities?.forEach((ent) => {
      links.push({
        source: {
          type: FEAST_FCO_TYPES["entity"],
          name: ent,
        },
        target: {
          type: FEAST_FCO_TYPES["featureView"],
          name: fv.spec?.name!,
        },
      });
    });

    // Data source relationships
    Object.values(fv.spec?.sources!).forEach(
      (input: { [key: string]: any }) => {
        if (input.requestDataSource) {
          links.push({
            source: {
              type: FEAST_FCO_TYPES["dataSource"],
              name: input.requestDataSource.name,
            },
            target: {
              type: FEAST_FCO_TYPES["featureView"],
              name: fv.spec?.name!,
            },
          });
        } else if (input.featureViewProjection?.featureViewName) {
          const source_fv = objects.featureViews?.find(
            (el) =>
              el.spec?.name === input.featureViewProjection.featureViewName,
          );
          if (!source_fv) {
            return;
          }
          links.push({
            source: {
              type: FEAST_FCO_TYPES["dataSource"],
              name: source_fv.spec?.batchSource?.name || "",
            },
            target: {
              type: FEAST_FCO_TYPES["featureView"],
              name: fv.spec?.name!,
            },
          });
        }
      },
    );
  });

  objects.streamFeatureViews?.forEach((fv) => {
    // stream source
    links.push({
      source: {
        type: FEAST_FCO_TYPES["dataSource"],
        name: fv.spec?.streamSource?.name!,
      },
      target: {
        type: FEAST_FCO_TYPES["featureView"],
        name: fv.spec?.name!,
      },
    });

    // batch source
    links.push({
      source: {
        type: FEAST_FCO_TYPES["dataSource"],
        name: fv.spec?.batchSource?.name!,
      },
      target: {
        type: FEAST_FCO_TYPES["featureView"],
        name: fv.spec?.name!,
      },
    });
  });

  return links;
};

export default parseEntityRelationships;
export type { EntityRelation, EntityReference };
