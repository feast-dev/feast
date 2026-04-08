import { useQuery } from "react-query";

export interface MlflowRunData {
  run_id: string;
  run_name: string;
  status: string;
  start_time: number;
  feature_service: string | null;
  feature_views: string[];
  feature_refs: string[];
  retrieval_type: string | null;
  entity_count: string | null;
  mlflow_url: string;
}

interface MlflowRunsResponse {
  runs: MlflowRunData[];
  mlflow_uri: string | null;
  error?: string;
}

const useLoadMlflowRuns = () => {
  return useQuery<MlflowRunsResponse>(
    "mlflow-runs",
    () => {
      return fetch("/api/mlflow-runs")
        .then((res) => {
          if (!res.ok) {
            return { runs: [], mlflow_uri: null };
          }
          return res.json();
        })
        .catch(() => {
          return { runs: [], mlflow_uri: null };
        });
    },
    {
      staleTime: 30000,
      retry: false,
    },
  );
};

export default useLoadMlflowRuns;
