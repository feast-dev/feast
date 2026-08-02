import { useCallback, useContext, useState } from "react";
import { useQueryClient } from "react-query";
import {
  ProjectListContext,
  ProjectsListSchema,
} from "../contexts/ProjectListContext";
import { useDataMode } from "../contexts/DataModeContext";

interface Toast {
  id: string;
  title: string;
  color: "success" | "danger";
  iconType: string;
}

const useRegistryRefresh = () => {
  const [refreshing, setRefreshing] = useState(false);
  const [toasts, setToasts] = useState<Toast[]>([]);
  const queryClient = useQueryClient();
  const projectListCtx = useContext(ProjectListContext);
  const basename = projectListCtx?.basename || "";
  const { fetchOptions } = useDataMode();

  const removeToast = useCallback((removedToast: { id: string }) => {
    setToasts((prev) => prev.filter((t) => t.id !== removedToast.id));
  }, []);

  const handleRefresh = useCallback(async () => {
    setRefreshing(true);
    try {
      const refreshRes = await fetch(`${basename}/api/v1/registry/refresh`, {
        method: "POST",
        headers: { ...fetchOptions?.headers },
        credentials: fetchOptions?.credentials,
      });
      if (!refreshRes.ok) {
        throw new Error(`Registry refresh failed (${refreshRes.status})`);
      }
      const res = await fetch(`${basename}/projects-list.json`, {
        headers: {
          "Content-Type": "application/json",
          ...fetchOptions?.headers,
        },
        credentials: fetchOptions?.credentials,
      });
      if (!res.ok) {
        throw new Error(`Failed to fetch project list (${res.status})`);
      }
      const json = await res.json();
      const parsed = ProjectsListSchema.parse(json);
      queryClient.setQueryData("feast-projects-list", parsed);
      await queryClient.invalidateQueries("registry-rest-bulk");
      setToasts((prev) => [
        ...prev,
        {
          id: String(Date.now()),
          title: "Refresh successful",
          color: "success" as const,
          iconType: "check",
        },
      ]);
    } catch {
      setToasts((prev) => [
        ...prev,
        {
          id: String(Date.now()),
          title: "Refresh failed",
          color: "danger" as const,
          iconType: "alert",
        },
      ]);
    } finally {
      setRefreshing(false);
    }
  }, [basename, queryClient, fetchOptions]);

  return { refreshing, toasts, handleRefresh, removeToast };
};

export default useRegistryRefresh;
