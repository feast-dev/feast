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

/**
 * Extract physical location identifiers (URIs, tables, paths) from a
 * SavedDatasetStorage JSON object (protobuf-JSON camelCase format).
 */
const extractStorageIdentifiers = (storage: any): Set<string> => {
  const ids = new Set<string>();
  if (!storage) return ids;
  if (storage.fileStorage?.uri) ids.add(storage.fileStorage.uri);
  if (storage.bigqueryStorage?.table) ids.add(storage.bigqueryStorage.table);
  if (storage.redshiftStorage?.table) ids.add(storage.redshiftStorage.table);
  if (storage.snowflakeStorage?.table) ids.add(storage.snowflakeStorage.table);
  if (storage.sparkStorage?.path) ids.add(storage.sparkStorage.path);
  if (storage.sparkStorage?.table) ids.add(storage.sparkStorage.table);
  if (storage.trinoStorage?.table) ids.add(storage.trinoStorage.table);
  if (storage.athenaStorage?.table) ids.add(storage.athenaStorage.table);
  return ids;
};

/**
 * Extract physical location identifiers from a DataSource JSON object.
 */
const extractDataSourceIdentifiers = (ds: any): Set<string> => {
  const ids = new Set<string>();
  if (!ds) return ids;
  if (ds.fileOptions?.uri) ids.add(ds.fileOptions.uri);
  if (ds.bigqueryOptions?.table) ids.add(ds.bigqueryOptions.table);
  if (ds.redshiftOptions?.table) ids.add(ds.redshiftOptions.table);
  if (ds.snowflakeOptions?.table) ids.add(ds.snowflakeOptions.table);
  if (ds.sparkOptions?.path) ids.add(ds.sparkOptions.path);
  if (ds.sparkOptions?.table) ids.add(ds.sparkOptions.table);
  if (ds.trinoOptions?.table) ids.add(ds.trinoOptions.table);
  if (ds.athenaOptions?.table) ids.add(ds.athenaOptions.table);
  // Embedded batch source
  if (ds.batchSource) {
    extractDataSourceIdentifiers(ds.batchSource).forEach((id) => ids.add(id));
  }
  return ids;
};

/**
 * Build a reverse index from physical location identifier → DataSource name.
 */
const buildDataSourceLocationIndex = (
  dataSources: any[],
): Map<string, string> => {
  const index = new Map<string, string>();
  dataSources?.forEach((ds: any) => {
    const name = ds.spec?.name || ds.name;
    if (!name) return;
    extractDataSourceIdentifiers(ds.spec || ds).forEach((id) => {
      index.set(id, name);
    });
  });
  return index;
};

const parseEntityRelationships = (objects: feast.core.Registry) => {
  const links: EntityRelation[] = [];

  const labelViewNames = new Set(
    ((objects as any).labelViews || []).map((lv: any) => lv.spec?.name),
  );

  objects.featureServices?.forEach((fs) => {
    fs.spec?.features!.forEach((feature: any) => {
      const viewName = feature?.featureViewName!;
      const isLabelView =
        feature?.viewType === "labelView" || labelViewNames.has(viewName);
      links.push({
        source: {
          type: isLabelView
            ? FEAST_FCO_TYPES["labelView"]
            : FEAST_FCO_TYPES["featureView"],
          name: viewName,
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

    // Source relationships — upstream feature views and request sources
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
          links.push({
            source: {
              type: FEAST_FCO_TYPES["featureView"],
              name: input.featureViewProjection.featureViewName,
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

  (objects as any).labelViews?.forEach((lv: any) => {
    lv.spec?.entities?.forEach((ent: string) => {
      links.push({
        source: {
          type: FEAST_FCO_TYPES["entity"],
          name: ent,
        },
        target: {
          type: FEAST_FCO_TYPES["labelView"],
          name: lv.spec?.name!,
        },
      });
    });

    if (lv.spec?.source?.name) {
      links.push({
        source: {
          type: FEAST_FCO_TYPES["dataSource"],
          name: lv.spec.source.name,
        },
        target: {
          type: FEAST_FCO_TYPES["labelView"],
          name: lv.spec?.name!,
        },
      });
    }
    if (lv.spec?.source?.batchSource?.name) {
      links.push({
        source: {
          type: FEAST_FCO_TYPES["dataSource"],
          name: lv.spec.source.batchSource.name,
        },
        target: {
          type: FEAST_FCO_TYPES["labelView"],
          name: lv.spec?.name!,
        },
      });
    }
    if (lv.spec?.batchSource?.name) {
      links.push({
        source: {
          type: FEAST_FCO_TYPES["dataSource"],
          name: lv.spec.batchSource.name,
        },
        target: {
          type: FEAST_FCO_TYPES["labelView"],
          name: lv.spec?.name!,
        },
      });
    }
  });

  // Build data source location index for storage-based matching
  const allDataSources = [
    ...((objects as any).dataSources || []),
    ...(objects.featureViews || [])
      .map((fv: any) => fv.spec?.batchSource)
      .filter(Boolean),
    ...(objects.streamFeatureViews || [])
      .flatMap((sfv: any) => [sfv.spec?.batchSource, sfv.spec?.streamSource])
      .filter(Boolean),
  ];
  const dsLocationIndex = buildDataSourceLocationIndex(allDataSources);

  (objects as any).savedDatasets?.forEach((sd: any) => {
    if (sd.spec?.featureServiceName) {
      links.push({
        source: {
          type: FEAST_FCO_TYPES["featureService"],
          name: sd.spec.featureServiceName,
        },
        target: {
          type: FEAST_FCO_TYPES["savedDataset"],
          name: sd.spec?.name!,
        },
      });
    }

    // FeatureView -> SavedDataset (derived from feature refs "view:feat")
    const seenViews = new Set<string>();
    sd.spec?.features?.forEach((featRef: string) => {
      const parts = featRef.split(":");
      const viewName = parts.length >= 2 ? parts[0] : featRef;
      if (viewName && !seenViews.has(viewName)) {
        seenViews.add(viewName);
        links.push({
          source: {
            type: FEAST_FCO_TYPES["featureView"],
            name: viewName,
          },
          target: {
            type: FEAST_FCO_TYPES["savedDataset"],
            name: sd.spec?.name!,
          },
        });
      }
    });

    // DataSource -> SavedDataset (matched by storage location)
    const storageIds = extractStorageIdentifiers(sd.spec?.storage);
    const matchedDsNames = new Set<string>();
    storageIds.forEach((locId) => {
      const dsName = dsLocationIndex.get(locId);
      if (dsName && !matchedDsNames.has(dsName)) {
        matchedDsNames.add(dsName);
        links.push({
          source: {
            type: FEAST_FCO_TYPES["dataSource"],
            name: dsName,
          },
          target: {
            type: FEAST_FCO_TYPES["savedDataset"],
            name: sd.spec?.name!,
          },
        });
      }
    });
  });

  return links;
};

export default parseEntityRelationships;
export type { EntityRelation, EntityReference };
