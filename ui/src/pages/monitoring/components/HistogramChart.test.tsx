import React from "react";
import { EuiProvider } from "@elastic/eui";
import { render, screen } from "@testing-library/react";
import type { NumericHistogram } from "../../../queries/useMonitoringApi";
import { NumericHistogramChart } from "./HistogramChart";

const histogram = (bins: number[], counts: number[]): NumericHistogram => ({
  bins,
  counts,
  bin_width: bins[1] - bins[0],
});

const renderChart = (current: NumericHistogram, baseline: NumericHistogram) =>
  render(
    <EuiProvider>
      <NumericHistogramChart histogram={current} baseline={baseline} />
    </EuiProvider>,
  );

const barHeights = (container: HTMLElement, fill: string) =>
  // Bar heights are SVG geometry, which has no accessible query.
  // eslint-disable-next-line testing-library/no-node-access
  Array.from(container.querySelectorAll(`rect[fill="${fill}"]`)).map((r) =>
    Number(r.getAttribute("height")),
  );

test("a baseline with many more rows is drawn at the window's scale", () => {
  // Same distribution; the baseline has 100x the rows of the daily window.
  const { container } = renderChart(
    histogram([0, 0.5, 1], [50, 50]),
    histogram([0, 0.5, 1], [5000, 5000]),
  );
  const windowBars = barHeights(container, "#006BB4");
  const baselineBars = barHeights(container, "#BD271E55");
  expect(baselineBars).toHaveLength(2);
  baselineBars.forEach((h, i) => expect(h).toBeCloseTo(windowBars[i]));
});

test("a shift past the baseline's range draws no baseline bars", () => {
  const { container } = renderChart(
    histogram([2, 51, 100], [60, 65]),
    histogram([0, 0.5, 1], [900, 900]),
  );
  expect(barHeights(container, "#BD271E55")).toHaveLength(0);
  expect(
    screen.getByText(/100% of baseline outside this range/),
  ).toBeInTheDocument();
});

test("a categorical baseline left from before a type change is not overlaid", () => {
  const categorical = {
    values: [{ value: "a", count: 10 }],
    other_count: 0,
    unique_count: 1,
  } as unknown as NumericHistogram;
  const { container } = renderChart(
    histogram([0, 0.5, 1], [5, 5]),
    categorical,
  );
  expect(barHeights(container, "#006BB4")).toHaveLength(2);
  expect(barHeights(container, "#BD271E55")).toHaveLength(0);
  expect(screen.queryByText(/^Baseline/)).not.toBeInTheDocument();
});
