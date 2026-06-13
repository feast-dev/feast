import React, { useState, useMemo } from "react";
import { useParams, useNavigate } from "react-router-dom";
import {
  EuiPageTemplate,
  EuiFlexGroup,
  EuiFlexItem,
  EuiSpacer,
  EuiSkeletonText,
  EuiEmptyPrompt,
  EuiButton,
  EuiBreadcrumbs,
  EuiSuperSelect,
  EuiFormRow,
} from "@elastic/eui";
import { FeatureIcon } from "../../graphics/FeatureIcon";
import {
  useFeatureMetrics,
  useBaselineMetrics,
} from "../../queries/useMonitoringApi";
import type {
  FeatureMetric,
  NumericHistogram,
  CategoricalHistogram,
} from "../../queries/useMonitoringApi";
import {
  NumericHistogramChart,
  CategoricalHistogramChart,
} from "./components/HistogramChart";
import StatsPanel from "./components/StatsPanel";
import TimeSeriesAnalysis from "./components/TimeSeriesAnalysis";
import { useDocumentTitle } from "../../hooks/useDocumentTitle";

const BASELINE_KEY = "__baseline__";

const GRANULARITY_LABELS: Record<string, string> = {
  daily: "Daily",
  weekly: "Weekly",
  biweekly: "Biweekly",
  monthly: "Monthly",
  quarterly: "Quarterly",
  [BASELINE_KEY]: "Baseline",
};

const FeatureMetricsDetail = () => {
  const { projectName, featureViewName, featureName } = useParams();
  const navigate = useNavigate();
  const [selectedGranularity, setSelectedGranularity] = useState("");

  useDocumentTitle(`${featureName} Monitoring | ${featureViewName} | Feast`);

  const {
    data: metrics,
    isLoading,
    isError,
  } = useFeatureMetrics({
    project: projectName || "",
    feature_view_name: featureViewName,
    feature_name: featureName,
  });

  const { data: baselineMetrics } = useBaselineMetrics(
    projectName || "",
    featureViewName,
    featureName,
  );

  const baselineMetric =
    baselineMetrics && baselineMetrics.length > 0 ? baselineMetrics[0] : null;

  const availableGranularities = useMemo(() => {
    const granularities = new Set<string>();
    if (metrics) {
      for (const m of metrics) {
        if (m.row_count > 0) granularities.add(m.granularity);
      }
    }
    return Array.from(granularities).sort();
  }, [metrics]);

  const granularityOptions = useMemo(() => {
    const options = availableGranularities.map((g) => ({
      value: g,
      inputDisplay: GRANULARITY_LABELS[g] || g,
      dropdownDisplay: GRANULARITY_LABELS[g] || g,
    }));
    if (baselineMetric) {
      options.push({
        value: BASELINE_KEY,
        inputDisplay: "Baseline",
        dropdownDisplay: "Baseline (all data)",
      });
    }
    return options;
  }, [availableGranularities, baselineMetric]);

  const effectiveGranularity =
    selectedGranularity || availableGranularities[0] || "";

  const activeMetric = useMemo(() => {
    if (effectiveGranularity === BASELINE_KEY && baselineMetric) {
      return baselineMetric;
    }
    if (!metrics || metrics.length === 0) return null;
    const matching = metrics.filter(
      (m) => m.granularity === effectiveGranularity && m.row_count > 0,
    );
    if (matching.length === 0) {
      const withData = metrics.filter((m) => m.row_count > 0);
      const candidates = withData.length > 0 ? withData : metrics;
      return candidates.reduce((a, b) =>
        a.metric_date > b.metric_date ? a : b,
      );
    }
    return matching.reduce((a, b) => (a.metric_date > b.metric_date ? a : b));
  }, [metrics, effectiveGranularity, baselineMetric]);

  const breadcrumbs = [
    {
      text: "Monitoring",
      onClick: () => navigate(`/p/${projectName}/monitoring`),
    },
    {
      text: featureViewName || "",
    },
    {
      text: featureName || "",
    },
  ];

  if (isLoading) {
    return (
      <EuiPageTemplate panelled>
        <EuiPageTemplate.Section>
          <EuiSkeletonText lines={8} />
        </EuiPageTemplate.Section>
      </EuiPageTemplate>
    );
  }

  if (isError || !activeMetric) {
    return (
      <EuiPageTemplate panelled>
        <EuiPageTemplate.Section>
          <EuiBreadcrumbs breadcrumbs={breadcrumbs} />
          <EuiSpacer />
          <EuiEmptyPrompt
            iconType="alert"
            title={<h2>No Metrics Available</h2>}
            body={
              <p>
                No monitoring metrics found for feature{" "}
                <strong>{featureName}</strong> in feature view{" "}
                <strong>{featureViewName}</strong>. Run a monitoring compute job
                first.
              </p>
            }
            actions={
              <EuiButton
                onClick={() => navigate(`/p/${projectName}/monitoring`)}
              >
                Back to Monitoring
              </EuiButton>
            }
          />
        </EuiPageTemplate.Section>
      </EuiPageTemplate>
    );
  }

  const isNumeric = activeMetric.feature_type === "numeric";

  return (
    <EuiPageTemplate panelled>
      <EuiPageTemplate.Header
        restrictWidth
        iconType={FeatureIcon}
        pageTitle={`${featureName}`}
        description={`Monitoring metrics for ${featureViewName} / ${featureName}`}
        rightSideItems={[
          <EuiButton
            key="back"
            size="s"
            onClick={() => navigate(`/p/${projectName}/monitoring`)}
          >
            Back to Monitoring
          </EuiButton>,
        ]}
      />
      <EuiPageTemplate.Section>
        <EuiBreadcrumbs breadcrumbs={breadcrumbs} />
        <EuiSpacer />

        {granularityOptions.length > 0 && (
          <>
            <EuiFlexGroup alignItems="flexEnd">
              <EuiFlexItem grow={false} style={{ minWidth: 200 }}>
                <EuiFormRow label="View" display="columnCompressed">
                  <EuiSuperSelect
                    options={granularityOptions}
                    valueOfSelected={effectiveGranularity}
                    onChange={(val) => setSelectedGranularity(val)}
                    compressed
                  />
                </EuiFormRow>
              </EuiFlexItem>
            </EuiFlexGroup>
            <EuiSpacer size="m" />
          </>
        )}

        <EuiFlexGroup gutterSize="l">
          <EuiFlexItem grow={2}>
            {isNumeric && activeMetric.histogram && (
              <NumericHistogramChart
                histogram={activeMetric.histogram as NumericHistogram}
                title={
                  effectiveGranularity === BASELINE_KEY
                    ? "Baseline Distribution"
                    : "Distribution"
                }
              />
            )}
            {!isNumeric && activeMetric.histogram && (
              <CategoricalHistogramChart
                histogram={activeMetric.histogram as CategoricalHistogram}
                title={
                  effectiveGranularity === BASELINE_KEY
                    ? "Baseline Category Distribution"
                    : "Category Distribution"
                }
              />
            )}
            {!activeMetric.histogram && (
              <EuiEmptyPrompt
                title={<h3>No Histogram Data</h3>}
                body={<p>Histogram data is not available for this metric.</p>}
              />
            )}
          </EuiFlexItem>

          <EuiFlexItem grow={1}>
            <StatsPanel metric={activeMetric} />
          </EuiFlexItem>
        </EuiFlexGroup>

        {metrics && metrics.length > 1 && (
          <>
            <EuiSpacer size="xl" />
            <TimeSeriesAnalysis
              metrics={metrics}
              featureType={activeMetric.feature_type}
            />
          </>
        )}
      </EuiPageTemplate.Section>
    </EuiPageTemplate>
  );
};

export default FeatureMetricsDetail;
