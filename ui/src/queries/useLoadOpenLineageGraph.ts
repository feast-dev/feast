import { useContext } from "react";
import { useQuery } from "react-query";
import RegistryPathContext from "../contexts/RegistryPathContext";
import { useDataMode } from "../contexts/DataModeContext";
import restFetch from "./restApiClient";

export interface OpenLineageNode {
  type: string;
  namespace: string;
  name: string;
  producer?: string;
  feast_object_type?: string;
  feast_object_name?: string;
  feast_project?: string;
  schema?: any;
  description?: string;
  job_type?: string;
  source_type?: string;
  facets?: Record<string, any>;
}

export interface OpenLineageEdge {
  source_type: string;
  source_namespace: string;
  source_name: string;
  target_type: string;
  target_namespace: string;
  target_name: string;
  edge_type?: string;
  updated_at?: number;
}

export interface OpenLineageSymlink {
  dataset_namespace: string;
  dataset_name: string;
  linked_namespace: string;
  linked_name: string;
  link_type: string;
}

export interface OpenLineageGraphData {
  nodes: OpenLineageNode[];
  edges: OpenLineageEdge[];
  symlinks?: OpenLineageSymlink[];
}

export interface OpenLineageEvent {
  event_id: string;
  event_type: string;
  event_time: number;
  producer?: string;
  job_namespace: string;
  job_name: string;
  run_id?: string;
  event_json: string;
  created_at: number;
}

export interface RegistryRelationship {
  source: { type: string; name: string };
  target: { type: string; name: string };
  type: string;
  project?: string;
}

export interface RegistryLineageData {
  relationships: RegistryRelationship[];
  indirect_relationships: RegistryRelationship[];
}

const useLoadOpenLineageGraph = () => {
  const registryUrl = useContext(RegistryPathContext);
  const { fetchOptions } = useDataMode();

  return useQuery<OpenLineageGraphData>(
    ["openlineage-graph"],
    () =>
      restFetch<OpenLineageGraphData>(
        registryUrl,
        "/lineage/openlineage/graph",
        fetchOptions,
      ),
    { enabled: !!registryUrl },
  );
};

const useLoadOpenLineageEvents = (
  namespace?: string,
  jobName?: string,
  limit: number = 100,
) => {
  const registryUrl = useContext(RegistryPathContext);
  const { fetchOptions } = useDataMode();

  const params = new URLSearchParams();
  if (namespace) params.set("namespace", namespace);
  if (jobName) params.set("job_name", jobName);
  params.set("limit", limit.toString());

  return useQuery<{ events: OpenLineageEvent[]; total: number }>(
    ["openlineage-events", namespace, jobName, limit],
    () =>
      restFetch(
        registryUrl,
        `/lineage/openlineage/events?${params.toString()}`,
        fetchOptions,
      ),
    { enabled: !!registryUrl },
  );
};

const useLoadRegistryLineage = (project?: string) => {
  const registryUrl = useContext(RegistryPathContext);
  const { fetchOptions } = useDataMode();

  return useQuery<RegistryLineageData>(
    ["registry-lineage", project],
    () =>
      restFetch<RegistryLineageData>(
        registryUrl,
        `/lineage/registry?project=${project}`,
        fetchOptions,
      ),
    { enabled: !!registryUrl && !!project },
  );
};

export {
  useLoadOpenLineageGraph,
  useLoadOpenLineageEvents,
  useLoadRegistryLineage,
};
