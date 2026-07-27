import { useMutation, useQueryClient } from "react-query";
import { restPost, restDelete } from "../restApiClient";

interface PolicyPayload {
  role_based_policy?: { roles: string[] };
  group_based_policy?: { groups: string[] };
  namespace_based_policy?: { namespaces: string[] };
  combined_group_namespace_policy?: {
    groups: string[];
    namespaces: string[];
  };
}

interface ApplyPermissionPayload {
  name: string;
  project: string;
  types: string[];
  name_patterns: string[];
  actions: string[];
  policy: PolicyPayload;
  tags?: Record<string, string>;
  required_tags?: Record<string, string>;
}

interface DeletePermissionPayload {
  name: string;
  project: string;
}

interface MutationResult {
  name: string;
  project: string;
  status: string;
}

const API_BASE = "/api/v1";

const useApplyPermission = () => {
  const queryClient = useQueryClient();

  return useMutation(
    (payload: ApplyPermissionPayload) =>
      restPost<MutationResult>(API_BASE, "/permissions", payload),
    {
      onSuccess: () => {
        queryClient.invalidateQueries(["rest"]);
        queryClient.invalidateQueries(["permissions-rest"]);
        queryClient.invalidateQueries(["registry-rest-bulk"]);
      },
    },
  );
};

const useDeletePermission = () => {
  const queryClient = useQueryClient();

  return useMutation(
    (payload: DeletePermissionPayload) =>
      restDelete<MutationResult>(
        API_BASE,
        `/permissions/${encodeURIComponent(payload.name)}?project=${encodeURIComponent(payload.project)}`,
      ),
    {
      onSuccess: () => {
        queryClient.invalidateQueries(["rest"]);
        queryClient.invalidateQueries(["permissions-rest"]);
        queryClient.invalidateQueries(["registry-rest-bulk"]);
      },
    },
  );
};

export { useApplyPermission, useDeletePermission };
export type { ApplyPermissionPayload, DeletePermissionPayload, PolicyPayload };
