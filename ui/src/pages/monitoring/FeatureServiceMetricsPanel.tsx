import React, { useState, useMemo } from "react";
import {
  EuiPanel,
  EuiTitle,
  EuiSpacer,
  EuiFlexGroup,
  EuiFlexItem,
  EuiStat,
  EuiBasicTable,
  EuiBasicTableColumn,
  EuiProgress,
  EuiBadge,
  EuiSkeletonText,
  Criteria,
} from "@elastic/eui";
import type { FeatureServiceMetric } from "../../queries/useMonitoringApi";

const healthColor = (nullRate: number): string => {
  if (nullRate >= 0.5) return "danger";
  if (nullRate >= 0.1) return "warning";
  return "success";
};

interface FeatureServiceMetricsPanelProps {
  metrics: FeatureServiceMetric[];
  isLoading: boolean;
}

const FeatureServiceMetricsPanel = ({
  metrics,
  isLoading,
}: FeatureServiceMetricsPanelProps) => {
  if (isLoading) {
    return (
      <EuiPanel hasBorder>
        <EuiTitle size="xs">
          <h3>Feature Service Metrics</h3>
        </EuiTitle>
        <EuiSpacer size="m" />
        <EuiSkeletonText lines={4} />
      </EuiPanel>
    );
  }

  const latestByFS = new Map<string, FeatureServiceMetric>();
  for (const m of metrics) {
    const existing = latestByFS.get(m.feature_service_name);
    if (!existing || m.metric_date > existing.metric_date) {
      latestByFS.set(m.feature_service_name, m);
    }
  }
  const latestMetrics = Array.from(latestByFS.values());

  const totalViews = latestMetrics.reduce(
    (sum, m) => sum + (m.total_feature_views || 0),
    0,
  );
  const totalFeatures = latestMetrics.reduce(
    (sum, m) => sum + (m.total_features || 0),
    0,
  );
  const avgNullRate =
    latestMetrics.length > 0
      ? latestMetrics.reduce((sum, m) => sum + (m.avg_null_rate || 0), 0) /
        latestMetrics.length
      : 0;

  const columns: EuiBasicTableColumn<FeatureServiceMetric>[] = [
    {
      field: "feature_service_name",
      name: "Feature Service",
      sortable: true,
    },
    {
      field: "total_feature_views",
      name: "Feature Views",
      sortable: true,
      width: "110px",
    },
    {
      field: "total_features",
      name: "Features",
      sortable: true,
      width: "80px",
    },
    {
      field: "avg_null_rate",
      name: "Avg Null Rate",
      sortable: true,
      render: (val: number) => (
        <div style={{ display: "flex", alignItems: "center", gap: 8 }}>
          <EuiProgress
            value={(val || 0) * 100}
            max={100}
            color={healthColor(val || 0) as any}
            size="s"
            style={{ width: 50 }}
          />
          <span>{((val || 0) * 100).toFixed(1)}%</span>
        </div>
      ),
    },
    {
      field: "max_null_rate",
      name: "Max Null Rate",
      sortable: true,
      width: "110px",
      render: (val: number) => `${((val || 0) * 100).toFixed(1)}%`,
    },
    {
      field: "metric_date",
      name: "Date",
      sortable: true,
      width: "110px",
    },
    {
      field: "data_source_type",
      name: "Source",
      width: "80px",
      render: (val: string) => <EuiBadge color="hollow">{val}</EuiBadge>,
    },
  ];

  return (
    <EuiPanel hasBorder>
      <EuiTitle size="xs">
        <h3>Feature Service Metrics</h3>
      </EuiTitle>
      <p style={{ color: "#69707D", fontSize: 13, marginTop: 4 }}>
        Aggregated data quality metrics across feature services.
      </p>
      <EuiSpacer size="m" />

      <EuiFlexGroup>
        <EuiFlexItem>
          <EuiStat
            title={latestMetrics.length.toString()}
            description="Services"
            titleSize="s"
            textAlign="center"
          />
        </EuiFlexItem>
        <EuiFlexItem>
          <EuiStat
            title={totalViews.toString()}
            description="Feature Views"
            titleSize="s"
            textAlign="center"
          />
        </EuiFlexItem>
        <EuiFlexItem>
          <EuiStat
            title={totalFeatures.toString()}
            description="Features"
            titleSize="s"
            textAlign="center"
          />
        </EuiFlexItem>
        <EuiFlexItem>
          <EuiStat
            title={`${(avgNullRate * 100).toFixed(1)}%`}
            description="Avg Null Rate"
            titleSize="s"
            titleColor={healthColor(avgNullRate)}
            textAlign="center"
          />
        </EuiFlexItem>
      </EuiFlexGroup>

      <EuiSpacer size="m" />

      {latestMetrics.length > 0 && (
        <SortableFSTable items={latestMetrics} columns={columns} />
      )}
    </EuiPanel>
  );
};

const SortableFSTable = ({
  items,
  columns,
}: {
  items: FeatureServiceMetric[];
  columns: EuiBasicTableColumn<FeatureServiceMetric>[];
}) => {
  const [sortField, setSortField] = useState<string>("feature_service_name");
  const [sortDirection, setSortDirection] = useState<"asc" | "desc">("asc");

  const sortedItems = useMemo(() => {
    return [...items].sort((a, b) => {
      const aVal = (a as any)[sortField];
      const bVal = (b as any)[sortField];
      if (aVal == null && bVal == null) return 0;
      if (aVal == null) return 1;
      if (bVal == null) return -1;
      if (aVal < bVal) return sortDirection === "asc" ? -1 : 1;
      if (aVal > bVal) return sortDirection === "asc" ? 1 : -1;
      return 0;
    });
  }, [items, sortField, sortDirection]);

  const onTableChange = ({ sort }: Criteria<FeatureServiceMetric>) => {
    if (sort) {
      setSortField(sort.field as string);
      setSortDirection(sort.direction);
    }
  };

  return (
    <EuiBasicTable
      items={sortedItems}
      columns={columns}
      sorting={{
        sort: {
          field: sortField as keyof FeatureServiceMetric,
          direction: sortDirection,
        },
      }}
      onChange={onTableChange}
      compressed
    />
  );
};

export default FeatureServiceMetricsPanel;
