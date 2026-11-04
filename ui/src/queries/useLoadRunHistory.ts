import { useContext } from "react";
import { useQuery } from "react-query";
import RegistryPathContext from "../contexts/RegistryPathContext";
import { useDataMode } from "../contexts/DataModeContext";
import restFetch from "./restApiClient";

export interface RunSummary {
  run_id: string;
  job_namespace: string;
  job_name: string;
  state: string;
  started_at: number | null;
  ended_at: number | null;
  updated_at: number;
}

export interface RunIOEntry {
  namespace: string;
  name: string;
  facets?: Record<string, any> | null;
}

export interface RunDetail extends RunSummary {
  inputs: RunIOEntry[];
  outputs: RunIOEntry[];
  facets?: Record<string, any> | null;
}

const useRunHistory = (jobNamespace?: string, jobName?: string) => {
  const registryUrl = useContext(RegistryPathContext);
  const { fetchOptions } = useDataMode();

  const params = new URLSearchParams();
  if (jobNamespace) params.set("job_namespace", jobNamespace);
  if (jobName) params.set("job_name", jobName);
  params.set("limit", "50");

  return useQuery<{ runs: RunSummary[]; total: number }>(
    ["openlineage-runs", jobNamespace, jobName],
    () =>
      restFetch(
        registryUrl,
        `/lineage/openlineage/runs?${params.toString()}`,
        fetchOptions,
      ),
    { enabled: !!registryUrl && !!jobNamespace && !!jobName },
  );
};

const useRunDetail = (runId?: string) => {
  const registryUrl = useContext(RegistryPathContext);
  const { fetchOptions } = useDataMode();

  return useQuery<RunDetail>(
    ["openlineage-run-detail", runId],
    () =>
      restFetch<RunDetail>(
        registryUrl,
        `/lineage/openlineage/runs/${runId}`,
        fetchOptions,
      ),
    { enabled: !!registryUrl && !!runId },
  );
};

export { useRunHistory, useRunDetail };
