import type { FeatureMetric } from "../../queries/useMonitoringApi";
import {
  latestWindowMetric,
  seriesForGranularity,
  windowGranularities,
  windowMetrics,
} from "./metricSelection";

const metric = (overrides: Partial<FeatureMetric>): FeatureMetric => ({
  project_id: "demo",
  feature_view_name: "driver_hourly_stats",
  feature_name: "conv_rate",
  metric_date: "2026-10-18",
  granularity: "daily",
  data_source_type: "batch",
  computed_at: "2026-10-19T18:40:50+00:00",
  max_event_timestamp: null,
  is_baseline: false,
  feature_type: "numeric",
  row_count: 125,
  null_count: 0,
  null_rate: 0,
  mean: null,
  stddev: null,
  min_val: null,
  max_val: null,
  p50: null,
  p75: null,
  p90: null,
  p95: null,
  p99: null,
  histogram: null,
  ...overrides,
});

// What `/monitoring/metrics/features` returns right after `feast monitor run`
// on the `feast init` demo: one row per granularity, plus the baseline, which
// is dated by the day it was computed.
const afterMonitorRun = [
  metric({
    granularity: "quarterly",
    metric_date: "2026-07-21",
    row_count: 1802,
  }),
  metric({
    granularity: "monthly",
    metric_date: "2026-09-19",
    row_count: 1802,
  }),
  metric({
    granularity: "biweekly",
    metric_date: "2026-10-05",
    row_count: 1687,
  }),
  metric({ granularity: "weekly", metric_date: "2026-10-12", row_count: 845 }),
  metric({ granularity: "daily", metric_date: "2026-10-18", row_count: 125 }),
  metric({
    granularity: "baseline",
    metric_date: "2026-10-20",
    is_baseline: true,
    row_count: 1807,
  }),
];

test("a baseline dated after the newest window is not the latest window", () => {
  expect(latestWindowMetric(afterMonitorRun)?.granularity).toBe("daily");
});

test("the baseline is shown when no window exists yet", () => {
  const baseline = metric({ granularity: "baseline", is_baseline: true });
  expect(latestWindowMetric([baseline])).toBe(baseline);
});

test("empty windows are skipped while others have data", () => {
  const empty = metric({ metric_date: "2026-10-19", row_count: 0 });
  const daily = metric({});
  expect(latestWindowMetric([daily, empty])).toBe(daily);
  expect(latestWindowMetric([empty])).toBe(empty);
});

test("no metrics gives no window", () => {
  expect(latestWindowMetric(undefined)).toBeNull();
  expect(latestWindowMetric([])).toBeNull();
});

test("window granularities exclude the baseline, shortest first", () => {
  expect(windowGranularities(afterMonitorRun)).toEqual([
    "daily",
    "weekly",
    "biweekly",
    "monthly",
    "quarterly",
  ]);
  expect(
    windowGranularities([metric({ granularity: "hourly" }), metric({})]),
  ).toEqual(["daily", "hourly"]);
});

test("a time series holds one granularity and no baseline", () => {
  const nextDay = metric({ metric_date: "2026-10-19" });
  const rows = [...afterMonitorRun, nextDay];
  expect(seriesForGranularity(rows, "daily").map((m) => m.metric_date)).toEqual(
    ["2026-10-18", "2026-10-19"],
  );
  expect(seriesForGranularity(rows, "baseline")).toEqual([]);
  expect(windowMetrics(rows)).toHaveLength(6);
});
