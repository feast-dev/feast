import React from "react";
import {
  EuiBadge,
  EuiBasicTable,
  EuiFlexGroup,
  EuiFlexItem,
  EuiHorizontalRule,
  EuiLoadingSpinner,
  EuiPanel,
  EuiText,
  EuiTitle,
  EuiToolTip,
} from "@elastic/eui";
import useLoadFeatureUsage, {
  FeatureUsageEntry,
} from "../../queries/useLoadFeatureUsage";

interface FeatureViewUsagePanelProps {
  featureViewName: string;
}

const formatTimestamp = (ts: number | null): string => {
  if (ts == null) return "Never";
  const date = new Date(ts);
  return date.toLocaleString();
};

const formatRelativeTime = (ts: number | null): string => {
  if (ts == null) return "";
  const now = Date.now();
  const diffMs = now - ts;
  const diffMin = Math.floor(diffMs / 60000);
  if (diffMin < 1) return "just now";
  if (diffMin < 60) return `${diffMin}m ago`;
  const diffHr = Math.floor(diffMin / 60);
  if (diffHr < 24) return `${diffHr}h ago`;
  const diffDays = Math.floor(diffHr / 24);
  return `${diffDays}d ago`;
};

const FeatureViewUsagePanel = ({
  featureViewName,
}: FeatureViewUsagePanelProps) => {
  const { data, isLoading, isError } = useLoadFeatureUsage();

  if (isLoading) {
    return (
      <EuiPanel hasBorder={true}>
        <EuiTitle size="xs">
          <h3>MLflow Usage</h3>
        </EuiTitle>
        <EuiHorizontalRule margin="xs" />
        <EuiFlexGroup justifyContent="center">
          <EuiFlexItem grow={false}>
            <EuiLoadingSpinner size="m" />
          </EuiFlexItem>
        </EuiFlexGroup>
      </EuiPanel>
    );
  }

  if (isError || !data) {
    return null;
  }

  const usage: FeatureUsageEntry | undefined =
    data.feature_usage?.[featureViewName];

  if (!usage || usage.run_count === 0) {
    return (
      <EuiPanel hasBorder={true}>
        <EuiTitle size="xs">
          <h3>MLflow Usage</h3>
        </EuiTitle>
        <EuiHorizontalRule margin="xs" />
        <EuiText size="s" color="subdued">
          No MLflow training runs have used this feature view.
        </EuiText>
      </EuiPanel>
    );
  }

  const modelItems = usage.models.map((name) => ({ name }));

  const modelColumns = [
    {
      name: "Registered Model",
      field: "name",
      render: (name: string) => (
        <EuiBadge color="hollow">{name}</EuiBadge>
      ),
    },
  ];

  return (
    <EuiPanel hasBorder={true}>
      <EuiTitle size="xs">
        <h3>MLflow Usage</h3>
      </EuiTitle>
      <EuiHorizontalRule margin="xs" />
      <EuiFlexGroup gutterSize="l">
        <EuiFlexItem grow={false}>
          <EuiText size="s">
            <strong>Training runs:</strong> {usage.run_count}
          </EuiText>
        </EuiFlexItem>
        <EuiFlexItem grow={false}>
          <EuiToolTip
            position="top"
            content={formatTimestamp(usage.last_used)}
          >
            <EuiText size="s">
              <strong>Last used:</strong>{" "}
              {formatRelativeTime(usage.last_used)}
            </EuiText>
          </EuiToolTip>
        </EuiFlexItem>
      </EuiFlexGroup>
      {modelItems.length > 0 && (
        <>
          <EuiHorizontalRule margin="s" />
          <EuiText size="xs" color="subdued">
            <strong>Registered models using this feature view:</strong>
          </EuiText>
          <EuiBasicTable
            columns={modelColumns}
            items={modelItems}
            compressed={true}
          />
        </>
      )}
    </EuiPanel>
  );
};

export default FeatureViewUsagePanel;
