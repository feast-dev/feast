import { useMutation, useQueryClient } from "react-query";

interface FeaturePayload {
  name: string;
  value_type: number;
  description?: string;
}

interface ApplyFeatureViewPayload {
  name: string;
  project: string;
  entities?: string[];
  features?: FeaturePayload[];
  batch_source?: string;
  ttl_seconds?: number;
  online?: boolean;
  description?: string;
  tags?: Record<string, string>;
  owner?: string;
}

interface DeleteFeatureViewPayload {
  name: string;
  project: string;
}

interface MutationResult {
  name: string;
  project: string;
  status: string;
}

const API_BASE = "/api/v1";

const applyFeatureView = async (
  payload: ApplyFeatureViewPayload,
): Promise<MutationResult> => {
  const response = await fetch(`${API_BASE}/feature_views`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(payload),
  });

  if (!response.ok) {
    const error = await response
      .json()
      .catch(() => ({ detail: response.statusText }));
    throw new Error(
      error.detail || `Failed to apply feature view: ${response.status}`,
    );
  }

  return response.json();
};

const deleteFeatureView = async (
  payload: DeleteFeatureViewPayload,
): Promise<MutationResult> => {
  const response = await fetch(
    `${API_BASE}/feature_views/${encodeURIComponent(payload.name)}?project=${encodeURIComponent(payload.project)}`,
    { method: "DELETE" },
  );

  if (!response.ok) {
    const error = await response
      .json()
      .catch(() => ({ detail: response.statusText }));
    throw new Error(
      error.detail || `Failed to delete feature view: ${response.status}`,
    );
  }

  return response.json();
};

const useApplyFeatureView = () => {
  const queryClient = useQueryClient();

  return useMutation(applyFeatureView, {
    onSuccess: () => {
      queryClient.invalidateQueries(["rest"]);
      queryClient.invalidateQueries(["feature-views-rest"]);
      queryClient.invalidateQueries(["feature-view-rest"]);
    },
  });
};

const useDeleteFeatureView = () => {
  const queryClient = useQueryClient();

  return useMutation(deleteFeatureView, {
    onSuccess: () => {
      queryClient.invalidateQueries(["rest"]);
      queryClient.invalidateQueries(["feature-views-rest"]);
      queryClient.invalidateQueries(["feature-view-rest"]);
    },
  });
};

export { useApplyFeatureView, useDeleteFeatureView };
export type { ApplyFeatureViewPayload, DeleteFeatureViewPayload };
