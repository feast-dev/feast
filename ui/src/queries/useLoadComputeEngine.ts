import { useContext } from "react";
import { useQuery } from "react-query";
import RegistryPathContext from "../contexts/RegistryPathContext";
import { useDataMode } from "../contexts/DataModeContext";
import restFetch, { RestApiError } from "./restApiClient";

export interface ComputeEngineConfig {
  type: string;
  [key: string]: any;
}

export interface ComputeEngineInfo {
  engineType: string;
  engineClass: string;
  config: ComputeEngineConfig;
  featureViewCount: number;
}

export interface FeatureViewEngineInfo {
  name: string;
  type: string;
  online: boolean;
  lastMaterialized?: string;
  hasOverride: boolean;
  overrides?: Record<string, any>;
  materializationIntervals: Array<{
    startTime?: string;
    endTime?: string;
    start_time?: string;
    end_time?: string;
  }>;
}

const ENGINE_TYPE_TO_CLASS: Record<string, string> = {
  local: "LocalComputeEngine",
  "spark.engine": "SparkComputeEngine",
  "ray.engine": "RayComputeEngine",
  "flink.engine": "FlinkComputeEngine",
  "snowflake.engine": "SnowflakeComputeEngine",
  lambda: "LambdaComputeEngine",
  k8s: "KubernetesComputeEngine",
};

function extractFeatureViewInfos(featureViews: any[]): FeatureViewEngineInfo[] {
  return featureViews.map((fv: any) => ({
    name: fv.name,
    type: fv.type || "Batch",
    online: fv.online ?? true,
    lastMaterialized: fv.lastMaterialized,
    hasOverride: fv.hasOverride ?? false,
    overrides: fv.overrides,
    materializationIntervals: fv.materializationIntervals || [],
  }));
}

export function useLoadComputeEngine(projectName?: string) {
  const registryUrl = useContext(RegistryPathContext);
  const { fetchOptions } = useDataMode();

  const enginePath =
    projectName && projectName !== "all"
      ? `/compute_engines?project=${encodeURIComponent(projectName)}`
      : "/compute_engines/all?limit=100";

  const engineQuery = useQuery<any, Error>(
    ["rest", "compute-engine", registryUrl, projectName || "all"],
    () => restFetch<any>(registryUrl, enginePath, fetchOptions),
    {
      enabled: !!registryUrl,
      staleTime: 30_000,
      retry: (failureCount, error) => {
        if (error instanceof RestApiError && error.status === 403) return false;
        return failureCount < 3;
      },
    },
  );

  const isPermissionDenied =
    engineQuery.isError &&
    engineQuery.error instanceof RestApiError &&
    engineQuery.error.status === 403;

  let engineInfo: ComputeEngineInfo | null = null;
  let featureViewInfos: FeatureViewEngineInfo[] = [];

  if (engineQuery.isSuccess && engineQuery.data) {
    const engine = engineQuery.data.engine || engineQuery.data.engines?.[0];
    if (engine) {
      engineInfo = {
        engineType: engine.engineType || "local",
        engineClass:
          engine.engineClass ||
          ENGINE_TYPE_TO_CLASS[engine.engineType] ||
          "LocalComputeEngine",
        config: engine.config || { type: engine.engineType || "local" },
        featureViewCount: engine.featureViewCount || 0,
      };
    }

    const rawFvs = engineQuery.data.featureViews || [];
    featureViewInfos = extractFeatureViewInfos(rawFvs);
  }

  if (!engineInfo) {
    engineInfo = {
      engineType: "local",
      engineClass: "LocalComputeEngine",
      config: { type: "local" },
      featureViewCount: featureViewInfos.length,
    };
  }

  return {
    isLoading: engineQuery.isLoading,
    isSuccess: engineQuery.isSuccess,
    isError: engineQuery.isError,
    isPermissionDenied,
    engineInfo,
    featureViewInfos,
  };
}
