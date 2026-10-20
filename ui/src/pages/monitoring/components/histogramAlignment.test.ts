import type { NumericHistogram } from "../../../queries/useMonitoringApi";
import { alignBaselineToBins } from "./histogramAlignment";

const histogram = (bins: number[], counts: number[]): NumericHistogram => ({
  bins,
  counts,
  bin_width: bins.length > 1 ? bins[1] - bins[0] : 0,
});

const total = (values: number[]) => values.reduce((a, b) => a + b, 0);

test("identical histograms align bin for bin", () => {
  const h = histogram([0, 1, 2, 3], [10, 30, 60]);
  const aligned = alignBaselineToBins(h, h);
  expect(aligned.counts).toEqual([10, 30, 60]);
  expect(aligned.outsideShare).toBe(0);
});

test("baseline is scaled to the window's row count", () => {
  const baseline = histogram([0, 1, 2], [500, 500]);
  const current = histogram([0, 1, 2], [3, 7]);
  const aligned = alignBaselineToBins(current, baseline);
  expect(aligned.counts[0]).toBeCloseTo(5);
  expect(aligned.counts[1]).toBeCloseTo(5);
  expect(total(aligned.counts)).toBeCloseTo(total(current.counts));
});

test("baseline mass is moved onto the window's own edges", () => {
  // Uniform baseline over [0, 10]; the window only spans [0, 5].
  const baseline = histogram([0, 5, 10], [50, 50]);
  const current = histogram([0, 2.5, 5], [40, 60]);
  const aligned = alignBaselineToBins(current, baseline);
  // A quarter of the baseline falls in each window bin; the rest is above 5.
  expect(aligned.counts[0]).toBeCloseTo(25);
  expect(aligned.counts[1]).toBeCloseTo(25);
  expect(aligned.outsideShare).toBeCloseTo(0.5);
});

test("a shift past the baseline's range leaves all of it outside the window", () => {
  const baseline = histogram([0, 0.5, 1], [900, 900]);
  const current = histogram([2, 51, 100], [60, 65]);
  const aligned = alignBaselineToBins(current, baseline);
  expect(aligned.counts).toEqual([0, 0]);
  expect(aligned.outsideShare).toBe(1);
});

test("index-matched bins over different ranges are not compared", () => {
  // Same counts per index, but the window covers only the baseline's top half.
  const baseline = histogram([0, 1, 2], [100, 100]);
  const current = histogram([1, 1.5, 2], [100, 100]);
  const aligned = alignBaselineToBins(current, baseline);
  expect(aligned.counts).toEqual([50, 50]);
  expect(aligned.outsideShare).toBeCloseTo(0.5);
});

test("constant columns stored as one zero-width bin are point masses", () => {
  const point = histogram([7, 7], [100]);
  expect(alignBaselineToBins(point, point)).toEqual({
    counts: [100],
    outsideShare: 0,
  });
  const spread = histogram([6, 7, 8], [40, 60]);
  // numpy puts a value on an interior edge in the right-hand bin.
  expect(alignBaselineToBins(spread, point).counts).toEqual([0, 100]);
  // A constant window takes the baseline bin holding its value, [7, 8).
  expect(alignBaselineToBins(point, spread)).toEqual({
    counts: [60],
    outsideShare: 0.4,
  });
});

test("a constant window of a 0/1 flag compares with the baseline's zeros", () => {
  // 90% of the baseline is 0; one day every value is 0, stored as one zero-width bin.
  const baseline = histogram([0, 0.5, 1], [90, 10]);
  const aligned = alignBaselineToBins(histogram([0, 0], [50]), baseline);
  expect(aligned.counts[0]).toBeCloseTo(45);
  expect(aligned.outsideShare).toBeCloseTo(0.1);
  // A constant window at the baseline's maximum falls in its closed last bin.
  expect(
    alignBaselineToBins(histogram([1, 1], [5]), baseline).outsideShare,
  ).toBe(0.9);
});

test("a value on the window's last edge stays inside", () => {
  const baseline = histogram([8, 8], [10]);
  const current = histogram([6, 7, 8], [5, 5]);
  expect(alignBaselineToBins(current, baseline).counts).toEqual([0, 10]);
});

test("an empty baseline adds nothing", () => {
  const aligned = alignBaselineToBins(
    histogram([0, 1, 2], [4, 6]),
    histogram([0, 1, 2], [0, 0]),
  );
  expect(aligned).toEqual({ counts: [0, 0], outsideShare: 0 });
});

test("histograms that omit the last edge use bin_width", () => {
  const baseline: NumericHistogram = {
    bins: [0, 1],
    counts: [10, 10],
    bin_width: 1,
  };
  const current = histogram([0, 1, 2], [1, 1]);
  expect(alignBaselineToBins(current, baseline).counts).toEqual([1, 1]);
});
