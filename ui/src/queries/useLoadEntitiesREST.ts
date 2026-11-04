import { useQuery } from "react-query";
import { fetchApi } from "./restApi";

interface EntityListResponse {
  entities: any[];
  pagination: Record<string, any>;
  relationships?: Record<string, any[]>;
}

const useLoadEntitiesREST = (project: string) => {
  return useQuery(
    ["entities-rest", project],
    () =>
      fetchApi<EntityListResponse>("/entities", {
        project,
        allow_cache: "false",
      }),
    {
      enabled: !!project,
      staleTime: 30000,
    },
  );
};

const useLoadEntityREST = (name: string, project: string) => {
  return useQuery(
    ["entity-rest", name, project],
    () =>
      fetchApi<any>(`/entities/${encodeURIComponent(name)}`, {
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

export { useLoadEntitiesREST, useLoadEntityREST };
