import React from "react";
import { useParams } from "react-router-dom";
import {
  EuiFlexGroup,
  EuiFlexItem,
  EuiSpacer,
  EuiSkeletonText,
  EuiEmptyPrompt,
  EuiButton,
} from "@elastic/eui";
import {
  useFeatureMetrics,
  useBaselineMetrics,
} from "../../queries/useMonitoringApi";
import type {
  NumericHistogram,
  CategoricalHistogram,
} from "../../queries/useMonitoringApi";
import {
  NumericHistogramChart,
  CategoricalHistogramChart,
} from "../monitoring/components/HistogramChart";
import StatsPanel from "../monitoring/components/StatsPanel";
import { latestWindowMetric } from "../monitoring/metricSelection";

const FeatureMonitoringTab = () => {
  const { projectName, FeatureViewName, FeatureName } = useParams();

  const {
    data: metrics,
    isLoading,
    isError,
  } = useFeatureMetrics({
    project: projectName || "",
    feature_view_name: FeatureViewName,
    feature_name: FeatureName,
  });

  const { data: baselineMetrics } = useBaselineMetrics(
    projectName || "",
    FeatureViewName,
    FeatureName,
  );

  if (isLoading) {
    return <EuiSkeletonText lines={6} />;
  }

  const latestMetric = latestWindowMetric(metrics);
  const showingBaseline = latestMetric?.is_baseline ?? false;

  const baselineMetric =
    !showingBaseline && baselineMetrics && baselineMetrics.length > 0
      ? baselineMetrics[0]
      : null;

  if (isError || !latestMetric) {
    return (
      <EuiEmptyPrompt
        iconType="monitoringApp"
        title={<h3>No Monitoring Data</h3>}
        body={
          <p>
            No monitoring metrics available for this feature. Run a monitoring
            compute job to generate data quality metrics.
          </p>
        }
        actions={
          <EuiButton size="s" href={`/p/${projectName}/monitoring`}>
            Go to Monitoring
          </EuiButton>
        }
      />
    );
  }

  const isNumeric = latestMetric.feature_type === "numeric";

  return (
    <>
      <EuiFlexGroup gutterSize="l">
        <EuiFlexItem grow={2}>
          {isNumeric && latestMetric.histogram && (
            <NumericHistogramChart
              histogram={latestMetric.histogram as NumericHistogram}
              baseline={baselineMetric?.histogram as NumericHistogram | null}
              title={showingBaseline ? "Baseline Distribution" : "Distribution"}
            />
          )}
          {!isNumeric && latestMetric.histogram && (
            <CategoricalHistogramChart
              histogram={latestMetric.histogram as CategoricalHistogram}
              title={
                showingBaseline
                  ? "Baseline Category Distribution"
                  : "Category Distribution"
              }
            />
          )}
          {!latestMetric.histogram && (
            <EuiEmptyPrompt
              title={<h3>No Histogram</h3>}
              body={<p>Histogram data is not available.</p>}
            />
          )}
        </EuiFlexItem>
        <EuiFlexItem grow={1}>
          <StatsPanel metric={latestMetric} baseline={baselineMetric} />
        </EuiFlexItem>
      </EuiFlexGroup>
      <EuiSpacer />
    </>
  );
};

export default FeatureMonitoringTab;
