import { useQuery } from "react-query";
import { fetchApi } from "./restApi";

interface DataSourceListResponse {
  dataSources: any[];
  pagination: Record<string, any>;
  relationships?: Record<string, any[]>;
}

const useLoadDataSourcesREST = (project: string) => {
  return useQuery(
    ["data-sources-rest", project],
    () =>
      fetchApi<DataSourceListResponse>("/data_sources", {
        project,
        allow_cache: "false",
      }),
    {
      enabled: !!project,
      staleTime: 30000,
    },
  );
};

const useLoadDataSourceREST = (name: string, project: string) => {
  return useQuery(
    ["data-source-rest", name, project],
    () =>
      fetchApi<any>(`/data_sources/${encodeURIComponent(name)}`, {
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

export { useLoadDataSourcesREST, useLoadDataSourceREST };
