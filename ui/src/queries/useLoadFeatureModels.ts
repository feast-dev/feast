import { useQuery } from "react-query";

export interface FeatureModelInfo {
  model_name: string;
  version: string;
  stage: string;
  mlflow_url: string;
}

interface FeatureModelsResponse {
  feature_models: Record<string, FeatureModelInfo[]>;
  error?: string;
}

const useLoadFeatureModels = () => {
  return useQuery<FeatureModelsResponse>(
    "feature-models",
    () => {
      return fetch("/api/mlflow-feature-models")
        .then((res) => {
          if (!res.ok) {
            return { feature_models: {} };
          }
          return res.json();
        })
        .catch(() => {
          return { feature_models: {} };
        });
    },
    {
      staleTime: 60000,
      retry: false,
    },
  );
};

export default useLoadFeatureModels;
