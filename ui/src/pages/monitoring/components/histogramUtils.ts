import type {
  NumericHistogram,
  CategoricalHistogram,
} from "../../../queries/useMonitoringApi";

/**
 * Rows the monitoring API returns for the saved baseline are dated by the day
 * the baseline was computed, while real windows are dated by their start, so
 * any "newest row" or "all rows" view must exclude them explicitly (#7054).
 */
const isBaselineMetricRow = (m: {
  granularity: string;
  is_baseline?: boolean;
}): boolean => m.is_baseline === true || m.granularity === "baseline";

const isNumericHistogram = (
  histogram: NumericHistogram | CategoricalHistogram | null | undefined,
): histogram is NumericHistogram =>
  !!histogram &&
  Array.isArray((histogram as NumericHistogram).counts) &&
  Array.isArray((histogram as NumericHistogram).bins);

/**
 * The newest real monitoring window, preferring rows with data. Baseline rows
 * never win: they are dated by computation day, not by window start.
 */
const selectLatestWindowMetric = <T extends {
  granularity: string;
  is_baseline?: boolean;
  metric_date: string;
  row_count: number;
}>(
  metrics: T[] | null | undefined,
): T | null => {
  if (!metrics || metrics.length === 0) return null;
  const windows = metrics.filter((m) => !isBaselineMetricRow(m));
  if (windows.length === 0) return null;
  const withData = windows.filter((m) => m.row_count > 0);
  const candidates = withData.length > 0 ? withData : windows;
  return candidates.reduce((a, b) => (a.metric_date > b.metric_date ? a : b));
};

/**
 * Re-bin a histogram onto another histogram's bin edges, distributing each
 * source bin's count across the target bins it overlaps in proportion to the
 * overlapped value range. Each stored histogram covers its own row's min and
 * max, so baseline bin i and window bin i do not line up (#7054).
 */
const rebinNumericHistogram = (
  source: NumericHistogram,
  target: NumericHistogram,
): number[] => {
  const targetCount = target.counts.length;
  const result = new Array<number>(targetCount).fill(0);
  for (let i = 0; i < targetCount; i++) {
    const t0 = target.bins[i];
    const t1 =
      i + 1 < target.bins.length ? target.bins[i + 1] : t0 + target.bin_width;
    const tWidth = t1 - t0;
    if (!(tWidth > 0)) continue;
    for (let j = 0; j < source.counts.length; j++) {
      const s0 = source.bins[j];
      const s1 =
        j + 1 < source.bins.length ? source.bins[j + 1] : s0 + source.bin_width;
      const sWidth = s1 - s0;
      if (!(sWidth > 0)) continue;
      const overlap = Math.min(t1, s1) - Math.max(t0, s0);
      if (overlap > 0) {
        result[i] += source.counts[j] * (overlap / sWidth);
      }
    }
  }
  return result;
};

/**
 * Convert raw counts to densities (each bin's share of the total), so windows
 * with different row counts compare at the same scale (#7054).
 */
const histogramDensities = (counts: number[]): number[] => {
  const total = counts.reduce((a, b) => a + b, 0);
  if (total <= 0) return counts.map(() => 0);
  return counts.map((c) => c / total);
};

export {
  isBaselineMetricRow,
  isNumericHistogram,
  selectLatestWindowMetric,
  rebinNumericHistogram,
  histogramDensities,
};
