import type { FeatureMetric } from "../../queries/useMonitoringApi";

// Shortest window first, so pages default to the most recent detail.
const GRANULARITY_ORDER = [
  "daily",
  "weekly",
  "biweekly",
  "monthly",
  "quarterly",
];

const rank = (granularity: string) => {
  const i = GRANULARITY_ORDER.indexOf(granularity);
  return i === -1 ? GRANULARITY_ORDER.length : i;
};

const newest = (metrics: FeatureMetric[]) =>
  metrics.reduce((a, b) => (a.metric_date > b.metric_date ? a : b));

/** Window rows only: `/monitoring/metrics/features` also returns the baseline row. */
export const windowMetrics = (
  metrics: FeatureMetric[] | undefined,
): FeatureMetric[] => (metrics || []).filter((m) => !m.is_baseline);

/**
 * The newest computed window. Baseline rows are dated by the day the baseline
 * was computed, which is usually later than the newest window's start date, so
 * they are used only when no window exists yet.
 */
export const latestWindowMetric = (
  metrics: FeatureMetric[] | undefined,
): FeatureMetric | null => {
  if (!metrics || metrics.length === 0) return null;
  const windows = windowMetrics(metrics);
  const pool = windows.length > 0 ? windows : metrics;
  const withData = pool.filter((m) => m.row_count > 0);
  return newest(withData.length > 0 ? withData : pool);
};

/** Granularities that have a window with data, shortest window first. */
export const windowGranularities = (
  metrics: FeatureMetric[] | undefined,
): string[] =>
  Array.from(
    new Set(
      windowMetrics(metrics)
        .filter((m) => m.row_count > 0)
        .map((m) => m.granularity),
    ),
  ).sort((a, b) => rank(a) - rank(b) || a.localeCompare(b));

/** One granularity's windows, so a time series compares like with like. */
export const seriesForGranularity = (
  metrics: FeatureMetric[] | undefined,
  granularity: string | undefined,
): FeatureMetric[] =>
  windowMetrics(metrics).filter((m) => m.granularity === granularity);
