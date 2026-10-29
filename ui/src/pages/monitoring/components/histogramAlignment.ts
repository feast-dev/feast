import type { NumericHistogram } from "../../../queries/useMonitoringApi";

export interface AlignedBaseline {
  /** Expected rows per window bin under the baseline distribution, scaled to the window's row count. */
  counts: number[];
  /** Share of the baseline that lies outside the window's [first edge, last edge]. */
  outsideShare: number;
}

const edgesOf = (histogram: NumericHistogram): number[] => {
  const { bins, counts, bin_width } = histogram;
  if (bins.length === counts.length && counts.length > 0) {
    return [...bins, bins[bins.length - 1] + bin_width];
  }
  return bins;
};

const sum = (values: number[]) => values.reduce((total, v) => total + v, 0);

/**
 * Re-express a baseline histogram on the bins of the current window's histogram.
 *
 * Every monitoring window computes its own bin edges, so baseline bin i and
 * window bin i usually cover different value ranges, and the baseline holds
 * far more rows. This moves each baseline bin's count onto the window's bins in
 * proportion to their overlap (values assumed uniform within a bin; a
 * zero-width bin is a point mass), then scales the result to the window's row
 * count so the two can be drawn on one axis. A constant window stored as one
 * zero-width bin (the SQL backends' form) takes the whole baseline bin that
 * contains its value, the finest resolution the baseline offers.
 */
export const alignBaselineToBins = (
  current: NumericHistogram,
  baseline: NumericHistogram,
): AlignedBaseline => {
  const target = edgesOf(current);
  const source = edgesOf(baseline);
  const aligned = new Array<number>(current.counts.length).fill(0);
  const baselineTotal = sum(baseline.counts);
  if (baselineTotal <= 0 || aligned.length === 0) {
    return { counts: aligned, outsideShare: 0 };
  }

  const last = aligned.length - 1;
  const lastSource = baseline.counts.length - 1;
  const pointWindow = target.length === 2 && target[0] === target[1];
  let inside = 0;
  baseline.counts.forEach((count, j) => {
    const lo = source[j];
    const hi = source[j + 1];
    if (!(count > 0) || lo === undefined || hi === undefined) return;
    if (hi === lo) {
      // Point mass: numpy puts a value on an interior edge in the right-hand bin.
      const k = target.findIndex(
        (edge, i) =>
          i <= last &&
          edge <= lo &&
          (lo < target[i + 1] || (i === last && lo <= target[i + 1])),
      );
      if (k >= 0) {
        aligned[k] += count;
        inside += count;
      }
      return;
    }
    if (pointWindow) {
      const v = target[0];
      if (lo <= v && (v < hi || (j === lastSource && v <= hi))) {
        aligned[0] += count;
        inside += count;
      }
      return;
    }
    for (let k = 0; k <= last; k++) {
      const overlap = Math.min(hi, target[k + 1]) - Math.max(lo, target[k]);
      if (overlap > 0) {
        const moved = (count * overlap) / (hi - lo);
        aligned[k] += moved;
        inside += moved;
      }
    }
  });

  const scale = sum(current.counts) / baselineTotal;
  return {
    counts: aligned.map((c) => c * scale),
    outsideShare: Math.min(Math.max(1 - inside / baselineTotal, 0), 1),
  };
};
