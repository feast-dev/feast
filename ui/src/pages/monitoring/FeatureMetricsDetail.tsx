import React from "react";
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
} from "@elastic/eui";
import { FeatureIcon } from "../../graphics/FeatureIcon";
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
} from "./components/HistogramChart";
import StatsPanel from "./components/StatsPanel";
import { useDocumentTitle } from "../../hooks/useDocumentTitle";

const FeatureMetricsDetail = () => {
  const { projectName, featureViewName, featureName } = useParams();
  const navigate = useNavigate();

  useDocumentTitle(
    `${featureName} Monitoring | ${featureViewName} | Feast`,
  );

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

  const latestMetric = (() => {
    if (!metrics || metrics.length === 0) return null;
    const withData = metrics.filter((m) => m.row_count > 0);
    const candidates = withData.length > 0 ? withData : metrics;
    return candidates.reduce((a, b) =>
      a.metric_date > b.metric_date ? a : b,
    );
  })();

  const baselineMetric =
    baselineMetrics && baselineMetrics.length > 0
      ? baselineMetrics[0]
      : null;

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

  if (isError || !latestMetric) {
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
                <strong>{featureViewName}</strong>. Run a monitoring
                compute job first.
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

  const isNumeric = latestMetric.feature_type === "numeric";

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

        <EuiFlexGroup gutterSize="l">
          <EuiFlexItem grow={2}>
            {isNumeric && latestMetric.histogram && (
              <NumericHistogramChart
                histogram={latestMetric.histogram as NumericHistogram}
                baseline={
                  baselineMetric?.histogram as NumericHistogram | null
                }
                title="Distribution"
              />
            )}
            {!isNumeric && latestMetric.histogram && (
              <CategoricalHistogramChart
                histogram={latestMetric.histogram as CategoricalHistogram}
                title="Category Distribution"
              />
            )}
            {!latestMetric.histogram && (
              <EuiEmptyPrompt
                title={<h3>No Histogram Data</h3>}
                body={<p>Histogram data is not available for this metric.</p>}
              />
            )}
          </EuiFlexItem>

          <EuiFlexItem grow={1}>
            <StatsPanel
              metric={latestMetric}
              baseline={baselineMetric}
            />
          </EuiFlexItem>
        </EuiFlexGroup>

        {metrics && metrics.length > 1 && (
          <>
            <EuiSpacer size="xl" />
            <NullRateTimeline metrics={metrics} />
          </>
        )}
      </EuiPageTemplate.Section>
    </EuiPageTemplate>
  );
};

const NullRateTimeline = ({
  metrics,
}: {
  metrics: { metric_date: string; null_rate: number }[];
}) => {
  const sorted = [...metrics].sort(
    (a, b) => a.metric_date.localeCompare(b.metric_date),
  );
  const maxRate = Math.max(...sorted.map((m) => m.null_rate), 0.01);
  const chartWidth = Math.max(sorted.length * 50, 200);
  const chartHeight = 80;

  const points = sorted.map((m, i) => {
    const x = (i / Math.max(sorted.length - 1, 1)) * (chartWidth - 20) + 10;
    const y = chartHeight - (m.null_rate / maxRate) * (chartHeight - 10);
    return { x, y, ...m };
  });

  const polyline = points.map((p) => `${p.x},${p.y}`).join(" ");

  return (
    <div>
      <h4 style={{ fontSize: 14, fontWeight: 600, marginBottom: 8 }}>
        Null Rate Over Time
      </h4>
      <svg width={chartWidth} height={chartHeight + 20} role="img">
        <polyline
          points={polyline}
          fill="none"
          stroke="#006BB4"
          strokeWidth={2}
        />
        {points.map((p, i) => (
          <circle key={i} cx={p.x} cy={p.y} r={3} fill="#006BB4" />
        ))}
        <line
          x1={10}
          y1={chartHeight}
          x2={chartWidth - 10}
          y2={chartHeight}
          stroke="#D3DAE6"
        />
        {points.length > 0 && (
          <>
            <text x={10} y={chartHeight + 14} fontSize={9} fill="#69707D">
              {points[0].metric_date}
            </text>
            <text
              x={chartWidth - 80}
              y={chartHeight + 14}
              fontSize={9}
              fill="#69707D"
            >
              {points[points.length - 1].metric_date}
            </text>
          </>
        )}
      </svg>
    </div>
  );
};

export default FeatureMetricsDetail;
