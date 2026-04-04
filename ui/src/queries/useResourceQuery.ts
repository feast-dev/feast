import { useContext } from "react";
import { useQuery, UseQueryResult } from "react-query";
import RegistryPathContext from "../contexts/RegistryPathContext";
import { useDataMode } from "../contexts/DataModeContext";
import { useResolvedMode, fetchProto } from "./useLoadRegistry";
import restFetch from "./restApiClient";
import type { FeatureStoreAllData } from "./useLoadRegistry";
import { FEAST_FV_TYPES, genericFVType } from "../parsers/mergedFVTypes";

interface ResourceQueryOptions<T> {
  resourceType: string;
  project?: string;
  protoSelect: (data: FeatureStoreAllData) => T | undefined;
  restPath: string;
  restSelect?: (data: any) => T | undefined;
  enabled?: boolean;
}

/**
 * Generic mode-aware hook for fetching a specific resource slice.
 *
 * Proto mode: all callers sharing the same (registryUrl, project) key
 * hit one cached fetch; each caller uses `select` to extract its slice.
 *
 * REST mode: each caller fires its own lightweight endpoint request.
 */
function useResourceQuery<T>({
  resourceType,
  project,
  protoSelect,
  restPath,
  restSelect,
  enabled = true,
}: ResourceQueryOptions<T>): UseQueryResult<T | undefined> {
  const mode = useResolvedMode();
  const registryUrl = useContext(RegistryPathContext);
  const { fetchOptions } = useDataMode();

  const protoResult = useQuery<FeatureStoreAllData, Error, T | undefined>(
    ["proto-registry", registryUrl, project || "all"],
    () => fetchProto(registryUrl, project),
    {
      enabled: mode === "proto" && !!registryUrl && enabled,
      staleTime: Infinity,
      select: protoSelect,
    },
  );

  const restResult = useQuery<any, Error, T | undefined>(
    ["rest", resourceType, registryUrl, project || "all"],
    () => restFetch<any>(registryUrl, restPath, fetchOptions),
    {
      enabled: mode !== "proto" && !!registryUrl && enabled,
      staleTime: 30_000,
      select: restSelect,
    },
  );

  return (mode === "proto" ? protoResult : restResult) as UseQueryResult<
    T | undefined
  >;
}

// ---------------------------------------------------------------------------
// REST endpoint path builders
// ---------------------------------------------------------------------------

function entityListPath(project?: string): string {
  if (project && project !== "all") {
    return `/entities?project=${encodeURIComponent(project)}&include_relationships=true`;
  }
  return "/entities/all?limit=100&include_relationships=true";
}

function entityDetailPath(name: string, project: string): string {
  return `/entities/${encodeURIComponent(name)}?project=${encodeURIComponent(project)}&include_relationships=true`;
}

function featureViewListPath(project?: string): string {
  if (project && project !== "all") {
    return `/feature_views?project=${encodeURIComponent(project)}&include_relationships=true`;
  }
  return "/feature_views/all?limit=100&include_relationships=true";
}

function featureViewDetailPath(name: string, project: string): string {
  return `/feature_views/${encodeURIComponent(name)}?project=${encodeURIComponent(project)}&include_relationships=true`;
}

function featureServiceListPath(project?: string): string {
  if (project && project !== "all") {
    return `/feature_services?project=${encodeURIComponent(project)}&include_relationships=true`;
  }
  return "/feature_services/all?limit=100&include_relationships=true";
}

function featureServiceDetailPath(name: string, project: string): string {
  return `/feature_services/${encodeURIComponent(name)}?project=${encodeURIComponent(project)}&include_relationships=true`;
}

function dataSourceListPath(project?: string): string {
  if (project && project !== "all") {
    return `/data_sources?project=${encodeURIComponent(project)}&include_relationships=true`;
  }
  return "/data_sources/all?limit=100&include_relationships=true";
}

function dataSourceDetailPath(name: string, project: string): string {
  return `/data_sources/${encodeURIComponent(name)}?project=${encodeURIComponent(project)}&include_relationships=true`;
}

function savedDatasetListPath(project?: string): string {
  if (project && project !== "all") {
    return `/saved_datasets?project=${encodeURIComponent(project)}`;
  }
  return "/saved_datasets/all?limit=100";
}

function savedDatasetDetailPath(name: string, project: string): string {
  return `/saved_datasets/${encodeURIComponent(name)}?project=${encodeURIComponent(project)}`;
}

function featuresListPath(project?: string): string {
  if (project && project !== "all") {
    return `/features?project=${encodeURIComponent(project)}`;
  }
  return "/features/all?limit=100";
}

function featureDetailPath(
  featureViewName: string,
  featureName: string,
  project: string,
): string {
  return `/features/${encodeURIComponent(featureViewName)}/${encodeURIComponent(featureName)}?project=${encodeURIComponent(project)}`;
}

// ---------------------------------------------------------------------------
// REST response → mergedFVList converter
// ---------------------------------------------------------------------------

function restFeatureViewsToMergedList(resp: any): genericFVType[] {
  const featureViews = resp?.featureViews || [];
  return featureViews.map((fv: any) => {
    const fvType = fv.type;
    if (fvType === "onDemandFeatureView") {
      return {
        name: fv.spec?.name,
        type: FEAST_FV_TYPES.ondemand,
        features: fv.spec?.features || [],
        object: fv,
      };
    }
    if (fvType === "streamFeatureView") {
      return {
        name: fv.spec?.name,
        type: FEAST_FV_TYPES.stream,
        features: fv.spec?.features || [],
        object: fv,
      };
    }
    return {
      name: fv.spec?.name,
      type: FEAST_FV_TYPES.regular,
      features: fv.spec?.features || [],
      object: fv,
    };
  });
}

function restFeatureViewDetailToGeneric(resp: any): genericFVType | undefined {
  if (!resp || !resp.spec) return undefined;
  const fvType = resp.type;
  if (fvType === "onDemandFeatureView") {
    return {
      name: resp.spec.name,
      type: FEAST_FV_TYPES.ondemand,
      features: resp.spec.features || [],
      object: resp,
    };
  }
  if (fvType === "streamFeatureView") {
    return {
      name: resp.spec.name,
      type: FEAST_FV_TYPES.stream,
      features: resp.spec.features || [],
      object: resp,
    };
  }
  return {
    name: resp.spec.name,
    type: FEAST_FV_TYPES.regular,
    features: resp.spec.features || [],
    object: resp,
  };
}

export default useResourceQuery;
export {
  entityListPath,
  entityDetailPath,
  featureViewListPath,
  featureViewDetailPath,
  featureServiceListPath,
  featureServiceDetailPath,
  dataSourceListPath,
  dataSourceDetailPath,
  savedDatasetListPath,
  savedDatasetDetailPath,
  featuresListPath,
  featureDetailPath,
  restFeatureViewsToMergedList,
  restFeatureViewDetailToGeneric,
};
