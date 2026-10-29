import React from "react";
import { EuiProvider } from "@elastic/eui";
import { QueryClient, QueryClientProvider } from "react-query";
import { MemoryRouter, Route, Routes } from "react-router-dom";
import { http, HttpResponse } from "msw";
import { setupServer } from "msw/node";
import { fireEvent, render, screen } from "@testing-library/react";

import DataModeContext from "../../contexts/DataModeContext";
import FeatureMetricsDetail from "./FeatureMetricsDetail";
import type { FeatureMetric } from "../../queries/useMonitoringApi";

const row = (
  metric_date: string,
  granularity: string,
  mean: number,
): FeatureMetric => ({
  project_id: "demo",
  feature_view_name: "driver_stats",
  feature_name: "conv_rate",
  metric_date,
  granularity,
  data_source_type: "batch",
  computed_at: "2026-10-31T00:00:00Z",
  max_event_timestamp: null,
  is_baseline: granularity === "baseline",
  feature_type: "numeric",
  row_count: 100,
  null_count: 0,
  null_rate: 0,
  mean,
  stddev: 0.1,
  min_val: 0,
  max_val: 1,
  p50: 0.5,
  p75: null,
  p90: null,
  p95: 0.9,
  p99: null,
  histogram: null,
});

const daily = Array.from({ length: 30 }, (_, i) =>
  row(`2026-10-${String(i + 1).padStart(2, "0")}`, "daily", (i + 1) / 100),
);
const monthly = row("2026-09-01", "monthly", 0.3);
const baseline: FeatureMetric = {
  ...row("2026-10-31", "baseline", 0.47),
  histogram: {
    bins: [0, 0.1, 0.2, 0.3, 0.4],
    counts: [60, 30, 10, 4, 1],
    bin_width: 0.1,
  },
};

const server = setupServer(
  http.get("/api/v1/monitoring/metrics/features", () =>
    HttpResponse.json([...daily, monthly, baseline]),
  ),
  http.get("/api/v1/monitoring/metrics/baseline", () =>
    HttpResponse.json([baseline]),
  ),
);

beforeAll(() => server.listen());
afterAll(() => server.close());

test("monitoring charts show values on hover and zoom", async () => {
  render(
    <QueryClientProvider
      client={
        new QueryClient({ defaultOptions: { queries: { retry: false } } })
      }
    >
      <DataModeContext.Provider value={{}}>
        <EuiProvider>
          <MemoryRouter
            initialEntries={[
              "/p/demo/monitoring/feature/driver_stats/conv_rate",
            ]}
          >
            <Routes>
              <Route
                path="/p/:projectName/monitoring/feature/:featureViewName/:featureName"
                element={<FeatureMetricsDetail />}
              />
            </Routes>
          </MemoryRouter>
        </EuiProvider>
      </DataModeContext.Provider>
    </QueryClientProvider>,
  );

  // Only daily windows: the monthly row (09-01) and the baseline (10-31)
  // would stretch the range if they were plotted.
  expect(
    await screen.findByText(/Showing daily windows 2026-10-01 – 2026-10-30\./),
  ).toBeInTheDocument();
  // Axis labels are calendar days, with no time-zone hour added.
  expect(screen.getAllByText("10-01").length).toBeGreaterThan(0);
  expect(screen.queryByText(/\d{2}-\d{2} \d{2}:\d{2}/)).toBeNull();

  // Hovering a distribution bar shows its range and count. The compact
  // chart puts 5 bars of 92 px from x 54, so x 340 is the fourth bar.
  const histogram = screen.getByRole("img", { name: "Histogram" });
  fireEvent.mouseMove(histogram, { clientX: 340 });
  const tooltip = screen.getByRole("tooltip");
  expect(tooltip).toHaveTextContent("0.30 – 0.40");
  expect(tooltip).toHaveTextContent("Count4");
  fireEvent.mouseLeave(histogram);

  // Drag across the left half of the plot (x 60 to 450 of 780 wide).
  const [drift] = screen.getAllByTestId("time-series-chart");
  fireEvent.mouseDown(drift, { clientX: 60, button: 0 });
  fireEvent.mouseMove(drift, { clientX: 450 });
  fireEvent.mouseUp(drift, { clientX: 450 });
  expect(
    screen.getByText(/Showing daily windows 2026-10-01 – 2026-10-15\./),
  ).toBeInTheDocument();

  // Hovering the first point shows its date and mean.
  fireEvent.mouseMove(drift, { clientX: 60 });
  expect(screen.getByText("2026-10-01")).toBeInTheDocument();
  expect(screen.getByText("0.01")).toBeInTheDocument();

  // Zoomed in, scrolling sideways moves through the rest of the data.
  // 780 px is one plot width, so the window moves on by its own length.
  const [scroller] = screen.getAllByTestId("time-series-scroll");
  scroller.scrollLeft = 780;
  fireEvent.scroll(scroller);
  expect(
    screen.getByText(/Showing daily windows 2026-10-15 – 2026-10-30\./),
  ).toBeInTheDocument();

  fireEvent.doubleClick(drift);
  expect(
    screen.getByText(/Showing daily windows 2026-10-01 – 2026-10-30\./),
  ).toBeInTheDocument();
});
