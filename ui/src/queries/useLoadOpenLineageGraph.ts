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
  total_nodes?: number;
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

const useLoadOpenLineageGraph = (options?: {
  namespace?: string;
  limit?: number;
  offset?: number;
}) => {
  const registryUrl = useContext(RegistryPathContext);
  const { fetchOptions } = useDataMode();

  const params = new URLSearchParams();
  if (options?.namespace) params.set("namespace", options.namespace);
  if (options?.limit) params.set("limit", options.limit.toString());
  if (options?.offset) params.set("offset", options.offset.toString());
  const qs = params.toString();
  const path = qs
    ? `/lineage/openlineage/graph?${qs}`
    : "/lineage/openlineage/graph";

  return useQuery<OpenLineageGraphData>(
    ["openlineage-graph", options?.namespace, options?.limit, options?.offset],
    () => restFetch<OpenLineageGraphData>(registryUrl, path, fetchOptions),
    { enabled: !!registryUrl },
  );
};

const useLoadNamespaces = () => {
  const registryUrl = useContext(RegistryPathContext);
  const { fetchOptions } = useDataMode();

  return useQuery<{ namespaces: string[] }>(
    ["openlineage-namespaces"],
    () =>
      restFetch<{ namespaces: string[] }>(
        registryUrl,
        "/lineage/openlineage/namespaces",
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

export interface OpenLineageJob {
  job_namespace: string;
  job_name: string;
  job_type?: string | null;
  producer?: string | null;
  description?: string | null;
  latest_run_id?: string | null;
  updated_at: number;
  facets_json?: string | null;
}

const useLoadOpenLineageJobs = () => {
  const registryUrl = useContext(RegistryPathContext);
  const { fetchOptions } = useDataMode();

  return useQuery<{ jobs: OpenLineageJob[] }>(
    ["openlineage-jobs"],
    () =>
      restFetch<{ jobs: OpenLineageJob[] }>(
        registryUrl,
        "/lineage/openlineage/jobs",
        fetchOptions,
      ),
    { enabled: !!registryUrl },
  );
};

export {
  useLoadOpenLineageGraph,
  useLoadOpenLineageEvents,
  useLoadOpenLineageJobs,
  useLoadRegistryLineage,
  useLoadNamespaces,
};
