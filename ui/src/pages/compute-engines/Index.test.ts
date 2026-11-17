import { buildJobsFromFeatureViews } from "./Index";
import type { FeatureViewEngineInfo } from "../../queries/useLoadComputeEngine";

const featureView = (
  overrides: Partial<FeatureViewEngineInfo>,
): FeatureViewEngineInfo => ({
  name: "driver_stats",
  type: "Batch",
  online: true,
  hasOverride: false,
  materializationIntervals: [],
  ...overrides,
});

test("shows a running job for a materializing feature view", () => {
  const jobs = buildJobsFromFeatureViews([
    featureView({
      state: "MATERIALIZING",
      materializationIntervals: [
        {
          startTime: "2025-01-01T00:00:00Z",
          endTime: "2025-01-02T00:00:00Z",
        },
      ],
    }),
    featureView({ name: "idle_view", state: "AVAILABLE_ONLINE" }),
  ]);

  expect(jobs.map((j) => [j.id, j.status])).toEqual([
    ["driver_stats-running", "RUNNING"],
    ["driver_stats-0", "SUCCEEDED"],
  ]);
});
