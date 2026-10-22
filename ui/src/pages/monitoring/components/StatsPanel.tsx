import React from "react";
import {
  EuiPanel,
  EuiTitle,
  EuiSpacer,
  EuiDescriptionList,
  EuiDescriptionListTitle,
  EuiDescriptionListDescription,
  EuiFlexGroup,
  EuiFlexItem,
  EuiBadge,
} from "@elastic/eui";
import type { FeatureMetric } from "../../../queries/useMonitoringApi";

const formatNumber = (val: number | null, decimals = 4): string => {
  if (val === null || val === undefined) return "—";
  if (Number.isInteger(val)) return val.toLocaleString();
  return val.toFixed(decimals);
};

const formatPercent = (val: number | null): string => {
  if (val === null || val === undefined) return "—";
  return `${(val * 100).toFixed(2)}%`;
};

const StatsPanel = ({
  metric,
  baseline,
}: {
  metric: FeatureMetric;
  baseline?: FeatureMetric | null;
}) => {
  const isNumeric = metric.feature_type === "numeric";

  return (
    <EuiPanel hasBorder>
      <EuiFlexGroup justifyContent="spaceBetween" alignItems="center">
        <EuiFlexItem grow={false}>
          <EuiTitle size="xxs">
            <h4>Statistics</h4>
          </EuiTitle>
        </EuiFlexItem>
        <EuiFlexItem grow={false}>
          <EuiBadge color={isNumeric ? "primary" : "accent"}>
            {metric.feature_type}
          </EuiBadge>
        </EuiFlexItem>
      </EuiFlexGroup>
      <EuiSpacer size="s" />
      <EuiDescriptionList compressed>
        <EuiDescriptionListTitle>Row Count</EuiDescriptionListTitle>
        <EuiDescriptionListDescription>
          {formatNumber(metric.row_count, 0)}
          {baseline && (
            <span style={{ color: "#69707D", marginLeft: 8 }}>
              (baseline: {formatNumber(baseline.row_count, 0)})
            </span>
          )}
        </EuiDescriptionListDescription>

        <EuiDescriptionListTitle>Null Rate</EuiDescriptionListTitle>
        <EuiDescriptionListDescription>
          <span
            style={{
              color: metric.null_rate > 0.1 ? "#BD271E" : "inherit",
              fontWeight: metric.null_rate > 0.1 ? 600 : 400,
            }}
          >
            {formatPercent(metric.null_rate)}
          </span>
          {baseline && (
            <span style={{ color: "#69707D", marginLeft: 8 }}>
              (baseline: {formatPercent(baseline.null_rate)})
            </span>
          )}
        </EuiDescriptionListDescription>

        {isNumeric && (
          <>
            <EuiDescriptionListTitle>Mean</EuiDescriptionListTitle>
            <EuiDescriptionListDescription>
              {formatNumber(metric.mean)}
              {baseline && (
                <span style={{ color: "#69707D", marginLeft: 8 }}>
                  (baseline: {formatNumber(baseline.mean)})
                </span>
              )}
            </EuiDescriptionListDescription>

            <EuiDescriptionListTitle>Std Dev</EuiDescriptionListTitle>
            <EuiDescriptionListDescription>
              {formatNumber(metric.stddev)}
            </EuiDescriptionListDescription>

            <EuiDescriptionListTitle>Min / Max</EuiDescriptionListTitle>
            <EuiDescriptionListDescription>
              {formatNumber(metric.min_val)} / {formatNumber(metric.max_val)}
            </EuiDescriptionListDescription>

            <EuiDescriptionListTitle>Percentiles</EuiDescriptionListTitle>
            <EuiDescriptionListDescription>
              P50: {formatNumber(metric.p50)} | P75: {formatNumber(metric.p75)}{" "}
              | P90: {formatNumber(metric.p90)} | P95:{" "}
              {formatNumber(metric.p95)} | P99: {formatNumber(metric.p99)}
            </EuiDescriptionListDescription>
          </>
        )}

        <EuiDescriptionListTitle>Data Source</EuiDescriptionListTitle>
        <EuiDescriptionListDescription>
          {metric.data_source_type}
        </EuiDescriptionListDescription>

        <EuiDescriptionListTitle>Granularity</EuiDescriptionListTitle>
        <EuiDescriptionListDescription>
          {metric.granularity}
        </EuiDescriptionListDescription>

        <EuiDescriptionListTitle>Computed At</EuiDescriptionListTitle>
        <EuiDescriptionListDescription>
          {metric.computed_at
            ? new Date(metric.computed_at).toLocaleString()
            : "—"}
        </EuiDescriptionListDescription>
      </EuiDescriptionList>
    </EuiPanel>
  );
};

export default StatsPanel;
