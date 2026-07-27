import { useQuery } from "react-query";
import mergedFVTypes, { genericFVType } from "../parsers/mergedFVTypes";
import parseEntityRelationships, {
  EntityRelation,
} from "../parsers/parseEntityRelationships";
import parseIndirectRelationships from "../parsers/parseIndirectRelationships";
import { useDataMode } from "../contexts/DataModeContext";
import restFetch from "./restApiClient";
import type { FetchOptions } from "../contexts/DataModeContext";

interface FeatureStoreAllData {
  project: string;
  description?: string;
  objects: any;
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
// Shared post-processing (used by the bulk REST fetch)
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
                ? String(feature.valueType)
                : feature.valueType
              : "Unknown Type",
          project: fv?.spec?.project || fv?.project,
        })) || [],
    ) || [];

  let resolvedProjectName: string =
    projectName === "all"
      ? "All Projects"
      : projectName ||
        (objects.projects &&
        objects.projects.length > 0 &&
        objects.projects[0].spec &&
        objects.projects[0].spec.name
          ? objects.projects[0].spec.name
          : objects.project
            ? objects.project
            : "default");

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
    permissions: objects.permissions || [],
  };
};

// ---------------------------------------------------------------------------
// REST fetch strategy
// ---------------------------------------------------------------------------

const permissionSafeFetch = async <T>(
  apiBaseUrl: string,
  path: string,
  fallback: T,
  fetchOptions?: FetchOptions,
): Promise<T> => {
  try {
    return await restFetch<T>(apiBaseUrl, path, fetchOptions);
  } catch (err: any) {
    if (err?.status === 403 || err?.status === 401) {
      return fallback;
    }
    throw err;
  }
};

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

  const emptyList = (key: string) => ({ [key]: [] });

  const [
    entitiesResp,
    featureViewsResp,
    labelViewsResp,
    featureServicesResp,
    dataSourcesResp,
    savedDatasetsResp,
    projectsResp,
  ] = await Promise.all([
    permissionSafeFetch<any>(
      apiBaseUrl,
      useAllEndpoint
        ? "/entities/all?include_relationships=true"
        : `/entities${projectParam}&include_relationships=true`,
      emptyList("entities"),
      fetchOptions,
    ),
    permissionSafeFetch<any>(
      apiBaseUrl,
      useAllEndpoint
        ? "/feature_views/all?include_relationships=true"
        : `/feature_views${projectParam}&include_relationships=true`,
      emptyList("featureViews"),
      fetchOptions,
    ),
    permissionSafeFetch<any>(
      apiBaseUrl,
      useAllEndpoint
        ? "/label_views/all?include_relationships=true"
        : `/label_views${projectParam}&include_relationships=true`,
      emptyList("featureViews"),
      fetchOptions,
    ),
    permissionSafeFetch<any>(
      apiBaseUrl,
      useAllEndpoint
        ? "/feature_services/all?include_relationships=true"
        : `/feature_services${projectParam}&include_relationships=true`,
      emptyList("featureServices"),
      fetchOptions,
    ),
    permissionSafeFetch<any>(
      apiBaseUrl,
      useAllEndpoint
        ? "/data_sources/all?include_relationships=true"
        : `/data_sources${projectParam}&include_relationships=true`,
      emptyList("dataSources"),
      fetchOptions,
    ),
    permissionSafeFetch<any>(
      apiBaseUrl,
      useAllEndpoint
        ? "/saved_datasets/all?include_relationships=true"
        : `/saved_datasets${projectParam}&include_relationships=true`,
      emptyList("savedDatasets"),
      fetchOptions,
    ),
    permissionSafeFetch<any>(
      apiBaseUrl,
      "/projects",
      emptyList("projects"),
      fetchOptions,
    ),
  ]);

  const entities = entitiesResp.entities || [];
  const allFeatureViews = featureViewsResp.featureViews || [];
  const labelViews: any[] = labelViewsResp.featureViews || [];
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
    labelViews,
    featureServices,
    dataSources,
    savedDatasets,
    projects,
  };

  return assembleFeatureStoreData(objects, projectName);
};

// ---------------------------------------------------------------------------
// Public hook
// ---------------------------------------------------------------------------

const useLoadRegistry = (url: string, projectName?: string) => {
  const { fetchOptions } = useDataMode();

  return useQuery(
    ["registry-rest-bulk", url, projectName || "all"],
    () => fetchREST(url, projectName, fetchOptions),
    {
      staleTime: 30_000,
      enabled: !!url,
    },
  );
};

export default useLoadRegistry;
export type { FeatureStoreAllData };
