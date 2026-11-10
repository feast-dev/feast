import {
  isBaselineMetricRow,
  isNumericHistogram,
  selectLatestWindowMetric,
  rebinNumericHistogram,
  histogramDensities,
} from "./histogramUtils";
import type { NumericHistogram } from "../../../queries/useMonitoringApi";

const windowRow = (overrides: Record<string, unknown>) => ({
  granularity: "daily",
  is_baseline: false,
  metric_date: "2026-10-18",
  row_count: 100,
  ...overrides,
});

describe("isBaselineMetricRow", () => {
  it("marks rows by is_baseline flag or baseline granularity", () => {
    expect(
      isBaselineMetricRow({ granularity: "baseline", is_baseline: true }),
    ).toBe(true);
    expect(
      isBaselineMetricRow({ granularity: "baseline", is_baseline: false }),
    ).toBe(true);
    expect(isBaselineMetricRow({ granularity: "daily" })).toBe(false);
  });
});

describe("selectLatestWindowMetric", () => {
  it("never picks the baseline row even when it has the newest metric_date", () => {
    // The baseline is dated by computation day (today), windows by start (#7054).
    const baseline = windowRow({
      granularity: "baseline",
      is_baseline: true,
      metric_date: "2026-10-20",
      row_count: 1807,
    });
    const daily = windowRow({ metric_date: "2026-10-18" });
    const weekly = windowRow({
      granularity: "weekly",
      metric_date: "2026-10-12",
    });
    expect(selectLatestWindowMetric([baseline, daily, weekly])).toBe(daily);
  });

  it("returns null when only baseline rows exist", () => {
    const baseline = windowRow({ granularity: "baseline", is_baseline: true });
    expect(selectLatestWindowMetric([baseline])).toBeNull();
  });

  it("prefers rows with data, like the previous selection did", () => {
    const empty = windowRow({ metric_date: "2026-10-19", row_count: 0 });
    const withData = windowRow({ metric_date: "2026-10-18", row_count: 12 });
    expect(selectLatestWindowMetric([empty, withData])).toBe(withData);
  });

  it("returns null for empty input", () => {
    expect(selectLatestWindowMetric([])).toBeNull();
    expect(selectLatestWindowMetric(null)).toBeNull();
  });
});

describe("isNumericHistogram", () => {
  it("distinguishes numeric and categorical payloads", () => {
    expect(isNumericHistogram({ bins: [0, 1], counts: [5], bin_width: 1 })).toBe(
      true,
    );
    expect(
      isNumericHistogram({
        values: [{ value: "a", count: 3 }],
        other_count: 0,
        unique_count: 1,
      }),
    ).toBe(false);
    expect(isNumericHistogram(null)).toBe(false);
    expect(isNumericHistogram(undefined)).toBe(false);
  });
});

describe("rebinNumericHistogram", () => {
  it("maps a differently-ranged baseline onto the window's bin edges", () => {
    // Baseline covers [0, 2] in 2 bins of width 1; window covers [0, 1] in 2
    // bins of width 0.5 (the x100-unit-bug case from #7054, inverted).
    const baseline: NumericHistogram = {
      bins: [0, 1],
      counts: [10, 30],
      bin_width: 1,
    };
    const window: NumericHistogram = {
      bins: [0, 0.5],
      counts: [1, 1],
      bin_width: 0.5,
    };
    const rebinned = rebinNumericHistogram(baseline, window);
    // window bin [0,0.5) overlaps half of baseline bin [0,1): 5
    // window bin [0.5,1) overlaps the other half of [0,1): 5
    expect(rebinned[0]).toBeCloseTo(5);
    expect(rebinned[1]).toBeCloseTo(5);
    // Total mass is preserved when the target covers the source range.
    expect(rebinned[0] + rebinned[1]).toBeCloseTo(10);
  });

  it("keeps identical bins identical", () => {
    const h: NumericHistogram = { bins: [0, 1, 2], counts: [3, 7], bin_width: 1 };
    expect(rebinNumericHistogram(h, h)).toEqual([3, 7]);
  });
});

describe("histogramDensities", () => {
  it("normalizes to shares of the total so window size stops mattering", () => {
    expect(histogramDensities([125, 125])).toEqual([0.5, 0.5]);
    expect(histogramDensities([1807, 1807])).toEqual([0.5, 0.5]);
    expect(histogramDensities([0, 0])).toEqual([0, 0]);
  });
});
