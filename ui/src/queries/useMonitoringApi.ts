import { useContext } from "react";
import { useQuery, useMutation, useQueryClient } from "react-query";
import MonitoringContext from "../contexts/MonitoringContext";
import { useDataMode } from "../contexts/DataModeContext";
import type { FetchOptions } from "../contexts/DataModeContext";

interface FeatureMetric {
  project_id: string;
  feature_view_name: string;
  feature_name: string;
  metric_date: string;
  granularity: string;
  data_source_type: string;
  computed_at: string;
  is_baseline: boolean;
  feature_type: string;
  row_count: number;
  null_count: number;
  null_rate: number;
  mean: number | null;
  stddev: number | null;
  min_val: number | null;
  max_val: number | null;
  p50: number | null;
  p75: number | null;
  p90: number | null;
  p95: number | null;
  p99: number | null;
  histogram: NumericHistogram | CategoricalHistogram | null;
}

interface NumericHistogram {
  bins: number[];
  counts: number[];
  bin_width: number;
}

interface CategoricalHistogram {
  values: { value: string; count: number }[];
  other_count: number;
  unique_count: number;
}

interface FeatureViewMetric {
  project_id: string;
  feature_view_name: string;
  metric_date: string;
  granularity: string;
  data_source_type: string;
  computed_at: string;
  is_baseline: boolean;
  total_row_count: number;
  total_features: number;
  features_with_nulls: number;
  avg_null_rate: number;
  max_null_rate: number;
}

interface FeatureServiceMetric {
  project_id: string;
  feature_service_name: string;
  metric_date: string;
  granularity: string;
  data_source_type: string;
  computed_at: string;
  is_baseline: boolean;
  total_feature_views: number;
  total_features: number;
  avg_null_rate: number;
  max_null_rate: number;
}

interface MonitoringFilters {
  project: string;
  feature_view_name?: string;
  feature_name?: string;
  feature_service_name?: string;
  granularity?: string;
  data_source_type?: string;
  start_date?: string;
  end_date?: string;
  is_baseline?: boolean;
}

const toQueryParams = (
  filters: MonitoringFilters,
): Record<string, string | undefined> => {
  return {
    project: filters.project,
    feature_view_name: filters.feature_view_name,
    feature_name: filters.feature_name,
    feature_service_name: filters.feature_service_name,
    granularity: filters.granularity,
    data_source_type: filters.data_source_type,
    start_date: filters.start_date,
    end_date: filters.end_date,
    is_baseline: filters.is_baseline ? "true" : undefined,
  };
};

const buildQueryString = (params: Record<string, string | undefined>) => {
  const entries = Object.entries(params).filter(
    ([, v]) => v !== undefined && v !== "",
  );
  if (entries.length === 0) return "";
  return (
    "?" + entries.map(([k, v]) => `${k}=${encodeURIComponent(v!)}`).join("&")
  );
};

const fetchMonitoring = async <T>(
  baseUrl: string,
  path: string,
  params: Record<string, string | undefined>,
  fetchOptions?: FetchOptions,
): Promise<T> => {
  const qs = buildQueryString(params);
  const res = await fetch(`${baseUrl}${path}${qs}`, {
    method: "GET",
    headers: {
      Accept: "application/json",
      ...fetchOptions?.headers,
    },
    credentials: fetchOptions?.credentials,
  });
  if (!res.ok) {
    throw new Error(`Failed to fetch ${path}: ${res.status} ${res.statusText}`);
  }
  const text = await res.text();
  const sanitized = text
    .replace(/:\s*NaN/g, ": null")
    .replace(/:\s*Infinity/g, ": null")
    .replace(/:\s*-Infinity/g, ": null");
  return JSON.parse(sanitized);
};

const STALE_TIME = 30_000;

const useFeatureMetrics = (filters: MonitoringFilters) => {
  const { apiBaseUrl } = useContext(MonitoringContext);
  const { fetchOptions } = useDataMode();
  const path = filters.is_baseline
    ? "/monitoring/metrics/baseline"
    : "/monitoring/metrics/features";
  return useQuery<FeatureMetric[]>(
    ["monitoring-features", filters],
    () =>
      fetchMonitoring<FeatureMetric[]>(
        apiBaseUrl,
        path,
        toQueryParams(filters),
        fetchOptions,
      ),
    { staleTime: STALE_TIME, retry: 1 },
  );
};

const aggregateToFeatureViewMetrics = (
  features: FeatureMetric[],
): FeatureViewMetric[] => {
  const grouped = new Map<string, FeatureMetric[]>();
  for (const f of features) {
    const key = f.feature_view_name;
    if (!grouped.has(key)) grouped.set(key, []);
    grouped.get(key)!.push(f);
  }
  return Array.from(grouped.entries()).map(([fvName, feats]) => {
    const nullRates = feats.map((f) => f.null_rate ?? 0);
    const maxRowCount = Math.max(...feats.map((f) => f.row_count ?? 0));
    return {
      project_id: feats[0].project_id,
      feature_view_name: fvName,
      metric_date: feats[0].metric_date,
      granularity: feats[0].granularity,
      data_source_type: feats[0].data_source_type,
      computed_at: feats[0].computed_at,
      is_baseline: feats[0].is_baseline,
      total_row_count: maxRowCount,
      total_features: feats.length,
      features_with_nulls: feats.filter((f) => (f.null_count ?? 0) > 0).length,
      avg_null_rate:
        nullRates.length > 0
          ? nullRates.reduce((a, b) => a + b, 0) / nullRates.length
          : 0,
      max_null_rate: nullRates.length > 0 ? Math.max(...nullRates) : 0,
    };
  });
};

const useFeatureViewMetrics = (filters: MonitoringFilters) => {
  const { apiBaseUrl } = useContext(MonitoringContext);
  const { fetchOptions } = useDataMode();
  const isBaseline = !!filters.is_baseline;
  return useQuery<FeatureViewMetric[]>(
    ["monitoring-feature-views", filters],
    async () => {
      if (isBaseline) {
        const features = await fetchMonitoring<FeatureMetric[]>(
          apiBaseUrl,
          "/monitoring/metrics/baseline",
          toQueryParams(filters),
          fetchOptions,
        );
        return aggregateToFeatureViewMetrics(features);
      }
      return fetchMonitoring<FeatureViewMetric[]>(
        apiBaseUrl,
        "/monitoring/metrics/feature_views",
        toQueryParams(filters),
        fetchOptions,
      );
    },
    { staleTime: STALE_TIME, retry: 1 },
  );
};

const useFeatureServiceMetrics = (filters: MonitoringFilters) => {
  const { apiBaseUrl } = useContext(MonitoringContext);
  const { fetchOptions } = useDataMode();
  return useQuery<FeatureServiceMetric[]>(
    ["monitoring-feature-services", filters],
    () =>
      fetchMonitoring<FeatureServiceMetric[]>(
        apiBaseUrl,
        "/monitoring/metrics/feature_services",
        toQueryParams(filters),
        fetchOptions,
      ),
    { staleTime: STALE_TIME, retry: 1 },
  );
};

const useBaselineMetrics = (
  project: string,
  featureViewName?: string,
  featureName?: string,
  dataSourceType?: string,
) => {
  const { apiBaseUrl } = useContext(MonitoringContext);
  const { fetchOptions } = useDataMode();
  return useQuery<FeatureMetric[]>(
    ["monitoring-baseline", project, featureViewName, featureName],
    () =>
      fetchMonitoring<FeatureMetric[]>(
        apiBaseUrl,
        "/monitoring/metrics/baseline",
        {
          project,
          feature_view_name: featureViewName,
          feature_name: featureName,
          data_source_type: dataSourceType,
        },
        fetchOptions,
      ),
    { staleTime: STALE_TIME, retry: 1 },
  );
};

const useComputeMetrics = () => {
  const { apiBaseUrl } = useContext(MonitoringContext);
  const { fetchOptions } = useDataMode();
  const queryClient = useQueryClient();
  return useMutation(
    async (body: { project: string; feature_view_name?: string }) => {
      const res = await fetch(`${apiBaseUrl}/monitoring/auto_compute`, {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
          ...fetchOptions?.headers,
        },
        credentials: fetchOptions?.credentials,
        body: JSON.stringify(body),
      });
      if (!res.ok) {
        throw new Error(`Failed to trigger compute: ${res.status}`);
      }
      return res.json();
    },
    {
      onSuccess: () => {
        queryClient.invalidateQueries("monitoring-features");
        queryClient.invalidateQueries("monitoring-feature-views");
        queryClient.invalidateQueries("monitoring-feature-services");
      },
    },
  );
};

export {
  useFeatureMetrics,
  useFeatureViewMetrics,
  useFeatureServiceMetrics,
  useBaselineMetrics,
  useComputeMetrics,
};
export type {
  FeatureMetric,
  FeatureViewMetric,
  FeatureServiceMetric,
  NumericHistogram,
  CategoricalHistogram,
  MonitoringFilters,
};
