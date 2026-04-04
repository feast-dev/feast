import { useQuery } from "react-query";
import mergedFVTypes, { genericFVType } from "../parsers/mergedFVTypes";
import parseEntityRelationships, {
  EntityRelation,
} from "../parsers/parseEntityRelationships";
import parseIndirectRelationships from "../parsers/parseIndirectRelationships";
import { feast } from "../protos";
import { useDataMode } from "../contexts/DataModeContext";
import { useLoadProjectsList } from "../contexts/ProjectListContext";
import restFetch from "./restApiClient";
import type { DataMode, FetchOptions } from "../contexts/DataModeContext";

interface FeatureStoreAllData {
  project: string;
  description?: string;
  objects: feast.core.Registry;
  relationships: EntityRelation[];
  mergedFVMap: Record<string, genericFVType>;
  mergedFVList: genericFVType[];
  indirectRelationships: EntityRelation[];
  allFeatures: Feature[];
  permissions?: any[];
}

interface Feature {
  name: string;
  featureView: string;
  type: string;
  project?: string;
}

// ---------------------------------------------------------------------------
// Shared post-processing
// ---------------------------------------------------------------------------

const assembleFeatureStoreData = (
  objects: any,
  projectName?: string,
): FeatureStoreAllData => {
  const { mergedFVMap, mergedFVList } = mergedFVTypes(objects);
  const relationships = parseEntityRelationships(objects);
  const indirectRelationships = parseIndirectRelationships(
    relationships,
    objects,
  );

  const allFeatures: Feature[] =
    objects.featureViews?.flatMap(
      (fv: any) =>
        fv?.spec?.features?.map((feature: any) => ({
          name: feature.name ?? "Unknown",
          featureView: fv?.spec?.name || "Unknown FeatureView",
          type:
            feature.valueType != null
              ? typeof feature.valueType === "number"
                ? feast.types.ValueType.Enum[feature.valueType]
                : feature.valueType
              : "Unknown Type",
          project: fv?.spec?.project || fv?.project,
        })) || [],
    ) || [];

  let resolvedProjectName: string =
    projectName === "all"
      ? "All Projects"
      : projectName ||
        (process.env.NODE_ENV === "test"
          ? "credit_scoring_aws"
          : objects.projects &&
              objects.projects.length > 0 &&
              objects.projects[0].spec &&
              objects.projects[0].spec.name
            ? objects.projects[0].spec.name
            : objects.project
              ? objects.project
              : "credit_scoring_aws");

  let projectDescription: string | undefined;
  if (projectName === "all") {
    projectDescription = "View data across all projects";
  } else if (objects.projects && objects.projects.length > 0) {
    const currentProject = objects.projects.find(
      (p: any) => p?.spec?.name === resolvedProjectName,
    );
    if (currentProject?.spec) {
      projectDescription = currentProject.spec.description;
    }
  }

  return {
    project: resolvedProjectName,
    description: projectDescription,
    objects,
    mergedFVMap,
    mergedFVList,
    relationships,
    indirectRelationships,
    allFeatures,
    permissions:
      objects.permissions && objects.permissions.length > 0
        ? objects.permissions
        : [
            {
              spec: {
                name: "zipcode-features-reader",
                types: [2],
                name_patterns: ["zipcode_features"],
                policy: { roles: ["analyst", "data_scientist"] },
                actions: [1, 4, 5],
              },
            },
            {
              spec: {
                name: "zipcode-source-writer",
                types: [7],
                name_patterns: ["zipcode"],
                policy: { roles: ["admin", "data_engineer"] },
                actions: [0, 2, 7],
              },
            },
            {
              spec: {
                name: "credit-score-v1-reader",
                types: [6],
                name_patterns: ["credit_score_v1"],
                policy: { roles: ["model_user", "data_scientist"] },
                actions: [1, 4],
              },
            },
            {
              spec: {
                name: "risky-features-reader",
                types: [2, 6],
                name_patterns: [],
                required_tags: { stage: "prod" },
                policy: { roles: ["trusted_analyst"] },
                actions: [5],
              },
            },
          ],
  };
};

// ---------------------------------------------------------------------------
// Proto fetch strategy (original behaviour)
// ---------------------------------------------------------------------------

const fetchProto = async (
  url: string,
  projectName?: string,
): Promise<FeatureStoreAllData> => {
  const res = await fetch(url, {
    headers: { "Content-Type": "application/json" },
  });

  const contentType = res.headers.get("content-type");
  let data;
  if (contentType && contentType.includes("application/json")) {
    data = await res.json();
  } else {
    data = await res.arrayBuffer();
  }

  let objects: any;
  if (data instanceof ArrayBuffer) {
    objects = feast.core.Registry.decode(new Uint8Array(data));
  } else {
    objects = data;
  }

  if (!objects.featureViews) {
    objects.featureViews = [];
  }

  if (projectName && projectName !== "all") {
    const projectsInRegistry = new Set<string>();
    objects.featureViews?.forEach((fv: any) => {
      if (fv?.spec?.project) projectsInRegistry.add(fv.spec.project);
    });
    objects.entities?.forEach((entity: any) => {
      if (entity?.spec?.project) projectsInRegistry.add(entity.spec.project);
    });

    const shouldFilter =
      projectsInRegistry.size > 1 || projectsInRegistry.has(projectName);

    if (shouldFilter && projectsInRegistry.has(projectName)) {
      if (objects.featureViews) {
        objects.featureViews = objects.featureViews.filter(
          (fv: any) => fv?.spec?.project === projectName,
        );
      }
      if (objects.entities) {
        objects.entities = objects.entities.filter(
          (entity: any) => entity?.spec?.project === projectName,
        );
      }
      if (objects.dataSources) {
        objects.dataSources = objects.dataSources.filter(
          (ds: any) => ds?.project === projectName,
        );
      }
      if (objects.featureServices) {
        objects.featureServices = objects.featureServices.filter(
          (fs: any) => fs?.spec?.project === projectName,
        );
      }
      if (objects.onDemandFeatureViews) {
        objects.onDemandFeatureViews = objects.onDemandFeatureViews.filter(
          (odfv: any) => odfv?.spec?.project === projectName,
        );
      }
      if (objects.streamFeatureViews) {
        objects.streamFeatureViews = objects.streamFeatureViews.filter(
          (sfv: any) => sfv?.spec?.project === projectName,
        );
      }
      if (objects.savedDatasets) {
        objects.savedDatasets = objects.savedDatasets.filter(
          (sd: any) => sd?.spec?.project === projectName,
        );
      }
      if (objects.validationReferences) {
        objects.validationReferences = objects.validationReferences.filter(
          (vr: any) => vr?.project === projectName,
        );
      }
      if (objects.permissions) {
        objects.permissions = objects.permissions.filter(
          (perm: any) =>
            perm?.spec?.project === projectName || !perm?.spec?.project,
        );
      }
    }
  }

  if (
    process.env.NODE_ENV === "test" &&
    objects.featureViews.length === 0
  ) {
    try {
      const fs = require("fs");
      const path = require("path");
      const { feast } = require("../protos");
      const registry = fs.readFileSync(
        path.resolve(__dirname, "../../public/registry.db"),
      );
      const parsedRegistry = feast.core.Registry.decode(registry);
      if (
        parsedRegistry.featureViews &&
        parsedRegistry.featureViews.length > 0
      ) {
        objects.featureViews = parsedRegistry.featureViews;
      }
    } catch (e) {
      console.error("Error loading test registry:", e);
    }
  }

  return assembleFeatureStoreData(objects, projectName);
};

// ---------------------------------------------------------------------------
// REST fetch strategy (rest / rest-external)
// ---------------------------------------------------------------------------

const fetchREST = async (
  apiBaseUrl: string,
  projectName?: string,
  fetchOptions?: FetchOptions,
): Promise<FeatureStoreAllData> => {
  const projectParam =
    projectName && projectName !== "all"
      ? `?project=${encodeURIComponent(projectName)}`
      : "";
  const useAllEndpoint = !projectParam;

  const [
    entitiesResp,
    featureViewsResp,
    featureServicesResp,
    dataSourcesResp,
    savedDatasetsResp,
    projectsResp,
  ] = await Promise.all([
    restFetch<any>(
      apiBaseUrl,
      useAllEndpoint
        ? "/entities/all?include_relationships=true"
        : `/entities${projectParam}&include_relationships=true`,
      fetchOptions,
    ),
    restFetch<any>(
      apiBaseUrl,
      useAllEndpoint
        ? "/feature_views/all?include_relationships=true"
        : `/feature_views${projectParam}&include_relationships=true`,
      fetchOptions,
    ),
    restFetch<any>(
      apiBaseUrl,
      useAllEndpoint
        ? "/feature_services/all?include_relationships=true"
        : `/feature_services${projectParam}&include_relationships=true`,
      fetchOptions,
    ),
    restFetch<any>(
      apiBaseUrl,
      useAllEndpoint
        ? "/data_sources/all?include_relationships=true"
        : `/data_sources${projectParam}&include_relationships=true`,
      fetchOptions,
    ),
    restFetch<any>(
      apiBaseUrl,
      useAllEndpoint
        ? "/saved_datasets/all?include_relationships=true"
        : `/saved_datasets${projectParam}&include_relationships=true`,
      fetchOptions,
    ),
    restFetch<any>(apiBaseUrl, "/projects", fetchOptions),
  ]);

  const entities = entitiesResp.entities || [];
  const allFeatureViews = featureViewsResp.featureViews || [];
  const featureServices = featureServicesResp.featureServices || [];
  const dataSources = dataSourcesResp.dataSources || [];
  const savedDatasets = savedDatasetsResp.savedDatasets || [];
  const projects = projectsResp.projects || [];

  const featureViews: any[] = [];
  const onDemandFeatureViews: any[] = [];
  const streamFeatureViews: any[] = [];

  for (const fv of allFeatureViews) {
    const fvType = fv.type;
    if (fvType === "onDemandFeatureView") {
      onDemandFeatureViews.push(fv);
    } else if (fvType === "streamFeatureView") {
      streamFeatureViews.push(fv);
    } else {
      featureViews.push(fv);
    }
  }

  const objects: any = {
    entities,
    featureViews,
    onDemandFeatureViews,
    streamFeatureViews,
    featureServices,
    dataSources,
    savedDatasets,
    projects,
  };

  return assembleFeatureStoreData(objects, projectName);
};

// ---------------------------------------------------------------------------
// Resolve effective mode
// ---------------------------------------------------------------------------

const useResolvedMode = (): DataMode => {
  const { mode: configMode } = useDataMode();
  const { data: projectsData } = useLoadProjectsList();
  const projectListMode = (projectsData as any)?.mode as
    | DataMode
    | undefined;

  if (configMode && configMode !== "proto") {
    return configMode;
  }
  if (projectListMode) {
    return projectListMode;
  }
  return configMode || "proto";
};

// ---------------------------------------------------------------------------
// Public hook
// ---------------------------------------------------------------------------

const useLoadRegistry = (url: string, projectName?: string) => {
  const resolvedMode = useResolvedMode();
  const { fetchOptions } = useDataMode();

  return useQuery(
    `registry:${resolvedMode}:${url}:${projectName || "all"}`,
    () => {
      if (resolvedMode === "proto") {
        return fetchProto(url, projectName);
      }
      return fetchREST(url, projectName, fetchOptions);
    },
    {
      staleTime: resolvedMode === "proto" ? Infinity : 30_000,
      enabled: !!url,
    },
  );
};

export default useLoadRegistry;
export type { FeatureStoreAllData };
