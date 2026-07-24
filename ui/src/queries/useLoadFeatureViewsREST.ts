import { useQuery } from "react-query";
import { fetchApi } from "./restApi";

interface FeatureViewListResponse {
  featureViews: any[];
  pagination: Record<string, any>;
  relationships?: Record<string, any[]>;
}

const useLoadFeatureViewsREST = (project: string) => {
  return useQuery(
    ["feature-views-rest", project],
    () =>
      fetchApi<FeatureViewListResponse>("/feature_views", {
        project,
        allow_cache: "false",
      }),
    {
      enabled: !!project,
      staleTime: 30000,
    },
  );
};

const useLoadFeatureViewREST = (name: string, project: string) => {
  return useQuery(
    ["feature-view-rest", name, project],
    () =>
      fetchApi<any>(`/feature_views/${encodeURIComponent(name)}`, {
        project,
        include_relationships: "true",
        allow_cache: "false",
      }),
    {
      enabled: !!name && !!project,
      staleTime: 30000,
    },
  );
};

export { useLoadFeatureViewsREST, useLoadFeatureViewREST };
