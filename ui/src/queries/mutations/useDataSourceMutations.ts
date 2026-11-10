import { useMutation, useQueryClient } from "react-query";

interface ApplyDataSourcePayload {
  name: string;
  project: string;
  type?: number;
  timestamp_field?: string;
  created_timestamp_column?: string;
  description?: string;
  tags?: Record<string, string>;
  owner?: string;
  file_options?: { uri: string };
  bigquery_options?: { table: string; query: string };
  snowflake_options?: { table: string; database: string; schema_: string };
  redshift_options?: { table: string; database: string; schema_: string };
  kafka_options?: { kafka_bootstrap_servers: string; topic: string };
  spark_options?: { table: string; path: string };
  custom_options?: {
    configuration?: string;
    class_name?: string;
    config?: string;
  };
  data_source_class_type?: string;
}

interface DeleteDataSourcePayload {
  name: string;
  project: string;
}

interface MutationResult {
  name: string;
  project: string;
  status: string;
}

interface ValidateDataSourcePayload {
  name: string;
  project: string;
}

interface ValidateDataSourceResult {
  name: string;
  project: string;
  status: "success" | "failed" | "not_applicable" | "unavailable";
  message: string;
  // camelCase: the REST layer returns MessageToDict(proto) output.
  testedAt: string;
  columns?: { name: string; type: string }[];
}

const API_BASE = "/api/v1";

const applyDataSource = async (
  payload: ApplyDataSourcePayload,
): Promise<MutationResult> => {
  const response = await fetch(`${API_BASE}/data_sources`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(payload),
  });

  if (!response.ok) {
    const error = await response
      .json()
      .catch(() => ({ detail: response.statusText }));
    throw new Error(
      error.detail || `Failed to apply data source: ${response.status}`,
    );
  }

  return response.json();
};

const deleteDataSource = async (
  payload: DeleteDataSourcePayload,
): Promise<MutationResult> => {
  const response = await fetch(
    `${API_BASE}/data_sources/${encodeURIComponent(payload.name)}?project=${encodeURIComponent(payload.project)}`,
    { method: "DELETE" },
  );

  if (!response.ok) {
    const error = await response
      .json()
      .catch(() => ({ detail: response.statusText }));
    throw new Error(
      error.detail || `Failed to delete data source: ${response.status}`,
    );
  }

  return response.json();
};

const validateDataSource = async (
  payload: ValidateDataSourcePayload,
): Promise<ValidateDataSourceResult> => {
  const response = await fetch(
    `${API_BASE}/data_sources/${encodeURIComponent(payload.name)}/validate?project=${encodeURIComponent(payload.project)}`,
    { method: "POST" },
  );

  if (!response.ok) {
    const error = await response
      .json()
      .catch(() => ({ detail: response.statusText }));
    throw new Error(
      error.detail || `Failed to test connection: ${response.status}`,
    );
  }

  return response.json();
};

const useApplyDataSource = () => {
  const queryClient = useQueryClient();

  return useMutation(applyDataSource, {
    onSuccess: () => {
      queryClient.invalidateQueries(["rest"]);
      queryClient.invalidateQueries(["data-sources-rest"]);
      queryClient.invalidateQueries(["data-source-rest"]);
    },
  });
};

const useDeleteDataSource = () => {
  const queryClient = useQueryClient();

  return useMutation(deleteDataSource, {
    onSuccess: () => {
      queryClient.invalidateQueries(["rest"]);
      queryClient.invalidateQueries(["data-sources-rest"]);
      queryClient.invalidateQueries(["data-source-rest"]);
    },
  });
};

const useValidateDataSource = () => {
  return useMutation(validateDataSource);
};

export { useApplyDataSource, useDeleteDataSource, useValidateDataSource };
export type {
  ApplyDataSourcePayload,
  DeleteDataSourcePayload,
  ValidateDataSourceResult,
};
