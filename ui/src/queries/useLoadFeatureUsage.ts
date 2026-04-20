import { useQuery } from "react-query";

interface FeatureUsageEntry {
  run_count: number;
  last_used: number | null;
  models: string[];
}

interface FeatureUsageResponse {
  feature_usage: Record<string, FeatureUsageEntry>;
  error?: string;
}

const fetchFeatureUsage = async (): Promise<FeatureUsageResponse> => {
  const response = await fetch("/api/mlflow-feature-usage");
  if (!response.ok) {
    throw new Error(`Failed to fetch feature usage: ${response.statusText}`);
  }
  return response.json();
};

const useLoadFeatureUsage = () => {
  return useQuery<FeatureUsageResponse>(
    "mlflowFeatureUsage",
    fetchFeatureUsage,
    {
      staleTime: 5 * 60 * 1000,
      refetchOnWindowFocus: false,
    }
  );
};

export default useLoadFeatureUsage;
export type { FeatureUsageEntry, FeatureUsageResponse };
