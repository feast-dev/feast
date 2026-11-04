import { useMutation, useQueryClient } from "react-query";

interface ApplyEntityPayload {
  name: string;
  project: string;
  join_key?: string;
  value_type?: number;
  description?: string;
  tags?: Record<string, string>;
  owner?: string;
}

interface DeleteEntityPayload {
  name: string;
  project: string;
}

interface MutationResult {
  name: string;
  project: string;
  status: string;
}

const API_BASE = "/api/v1";

const applyEntity = async (
  payload: ApplyEntityPayload,
): Promise<MutationResult> => {
  const response = await fetch(`${API_BASE}/entities`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(payload),
  });

  if (!response.ok) {
    const error = await response
      .json()
      .catch(() => ({ detail: response.statusText }));
    throw new Error(
      error.detail || `Failed to apply entity: ${response.status}`,
    );
  }

  return response.json();
};

const deleteEntity = async (
  payload: DeleteEntityPayload,
): Promise<MutationResult> => {
  const response = await fetch(
    `${API_BASE}/entities/${encodeURIComponent(payload.name)}?project=${encodeURIComponent(payload.project)}`,
    { method: "DELETE" },
  );

  if (!response.ok) {
    const error = await response
      .json()
      .catch(() => ({ detail: response.statusText }));
    throw new Error(
      error.detail || `Failed to delete entity: ${response.status}`,
    );
  }

  return response.json();
};

const useApplyEntity = () => {
  const queryClient = useQueryClient();

  return useMutation(applyEntity, {
    onSuccess: () => {
      queryClient.invalidateQueries(["rest"]);
      queryClient.invalidateQueries(["entities-rest"]);
      queryClient.invalidateQueries(["entity-rest"]);
    },
  });
};

const useDeleteEntity = () => {
  const queryClient = useQueryClient();

  return useMutation(deleteEntity, {
    onSuccess: () => {
      queryClient.invalidateQueries(["rest"]);
      queryClient.invalidateQueries(["entities-rest"]);
      queryClient.invalidateQueries(["entity-rest"]);
    },
  });
};

export { useApplyEntity, useDeleteEntity };
export type { ApplyEntityPayload, DeleteEntityPayload };
