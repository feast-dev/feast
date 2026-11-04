import { useMutation, useQueryClient } from "react-query";

interface FeatureViewProjectionPayload {
  feature_view_name: string;
  feature_names?: string[];
}

interface ApplyFeatureServicePayload {
  name: string;
  project: string;
  features: FeatureViewProjectionPayload[];
  description?: string;
  tags?: Record<string, string>;
  owner?: string;
}

interface DeleteFeatureServicePayload {
  name: string;
  project: string;
}

interface MutationResult {
  name: string;
  project: string;
  status: string;
}

const API_BASE = "/api/v1";

const applyFeatureService = async (
  payload: ApplyFeatureServicePayload,
): Promise<MutationResult> => {
  const response = await fetch(`${API_BASE}/feature_services`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(payload),
  });

  if (!response.ok) {
    const error = await response
      .json()
      .catch(() => ({ detail: response.statusText }));
    throw new Error(
      error.detail || `Failed to apply feature service: ${response.status}`,
    );
  }

  return response.json();
};

const deleteFeatureService = async (
  payload: DeleteFeatureServicePayload,
): Promise<MutationResult> => {
  const response = await fetch(
    `${API_BASE}/feature_services/${encodeURIComponent(payload.name)}?project=${encodeURIComponent(payload.project)}`,
    { method: "DELETE" },
  );

  if (!response.ok) {
    const error = await response
      .json()
      .catch(() => ({ detail: response.statusText }));
    throw new Error(
      error.detail || `Failed to delete feature service: ${response.status}`,
    );
  }

  return response.json();
};

const useApplyFeatureService = () => {
  const queryClient = useQueryClient();

  return useMutation(applyFeatureService, {
    onSuccess: () => {
      queryClient.invalidateQueries(["rest"]);
      queryClient.invalidateQueries(["feature-services-rest"]);
      queryClient.invalidateQueries(["feature-service-rest"]);
    },
  });
};

const useDeleteFeatureService = () => {
  const queryClient = useQueryClient();

  return useMutation(deleteFeatureService, {
    onSuccess: () => {
      queryClient.invalidateQueries(["rest"]);
      queryClient.invalidateQueries(["feature-services-rest"]);
      queryClient.invalidateQueries(["feature-service-rest"]);
    },
  });
};

export { useApplyFeatureService, useDeleteFeatureService };
export type {
  ApplyFeatureServicePayload,
  DeleteFeatureServicePayload,
  FeatureViewProjectionPayload,
};
