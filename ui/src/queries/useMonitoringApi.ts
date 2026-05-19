import { useContext } from "react";
import { useQuery, useMutation, useQueryClient } from "react-query";
import MonitoringContext from "../contexts/MonitoringContext";

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
  };
};

const buildQueryString = (params: Record<string, string | undefined>) => {
  const entries = Object.entries(params).filter(
    ([, v]) => v !== undefined && v !== "",
  );
  if (entries.length === 0) return "";
  return "?" + entries.map(([k, v]) => `${k}=${encodeURIComponent(v!)}`).join("&");
};

const fetchMonitoring = async <T>(
  baseUrl: string,
  path: string,
  params: Record<string, string | undefined>,
): Promise<T> => {
  const qs = buildQueryString(params);
  const res = await fetch(`${baseUrl}${path}${qs}`);
  if (!res.ok) {
    throw new Error(`Failed to fetch ${path}: ${res.status} ${res.statusText}`);
  }
  const text = await res.text();
  const sanitized = text.replace(/:\s*NaN/g, ": null").replace(/:\s*Infinity/g, ": null").replace(/:\s*-Infinity/g, ": null");
  return JSON.parse(sanitized);
};

const STALE_TIME = 30_000;

const useFeatureMetrics = (filters: MonitoringFilters) => {
  const { apiBaseUrl, enabled } = useContext(MonitoringContext);
  return useQuery<FeatureMetric[]>(
    ["monitoring-features", filters],
    () =>
      fetchMonitoring<FeatureMetric[]>(
        apiBaseUrl,
        "/monitoring/metrics/features",
        toQueryParams(filters),
      ),
    { staleTime: STALE_TIME, enabled, retry: 1 },
  );
};

const useFeatureViewMetrics = (filters: MonitoringFilters) => {
  const { apiBaseUrl, enabled } = useContext(MonitoringContext);
  return useQuery<FeatureViewMetric[]>(
    ["monitoring-feature-views", filters],
    () =>
      fetchMonitoring<FeatureViewMetric[]>(
        apiBaseUrl,
        "/monitoring/metrics/feature_views",
        toQueryParams(filters),
      ),
    { staleTime: STALE_TIME, enabled, retry: 1 },
  );
};

const useFeatureServiceMetrics = (filters: MonitoringFilters) => {
  const { apiBaseUrl, enabled } = useContext(MonitoringContext);
  return useQuery<FeatureServiceMetric[]>(
    ["monitoring-feature-services", filters],
    () =>
      fetchMonitoring<FeatureServiceMetric[]>(
        apiBaseUrl,
        "/monitoring/metrics/feature_services",
        toQueryParams(filters),
      ),
    { staleTime: STALE_TIME, enabled, retry: 1 },
  );
};

const useBaselineMetrics = (
  project: string,
  featureViewName?: string,
  featureName?: string,
  dataSourceType?: string,
) => {
  const { apiBaseUrl, enabled } = useContext(MonitoringContext);
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
      ),
    { staleTime: STALE_TIME, enabled, retry: 1 },
  );
};

const useTimeseriesMetrics = (filters: MonitoringFilters) => {
  const { apiBaseUrl, enabled } = useContext(MonitoringContext);
  return useQuery<FeatureMetric[]>(
    ["monitoring-timeseries", filters],
    () =>
      fetchMonitoring<FeatureMetric[]>(
        apiBaseUrl,
        "/monitoring/metrics/timeseries",
        toQueryParams(filters),
      ),
    { staleTime: STALE_TIME, enabled, retry: 1 },
  );
};

const useComputeMetrics = () => {
  const { apiBaseUrl } = useContext(MonitoringContext);
  const queryClient = useQueryClient();
  return useMutation(
    async (body: {
      project: string;
      feature_view_name?: string;
      feature_names?: string[];
      start_date?: string;
      end_date?: string;
      granularity?: string;
      set_baseline?: boolean;
    }) => {
      const res = await fetch(`${apiBaseUrl}/monitoring/compute`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
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
  useTimeseriesMetrics,
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
