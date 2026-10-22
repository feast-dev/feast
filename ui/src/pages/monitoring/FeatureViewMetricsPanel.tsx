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
import type { FeatureViewMetric } from "../../queries/useMonitoringApi";

const healthColor = (nullRate: number): string => {
  if (nullRate >= 0.5) return "danger";
  if (nullRate >= 0.1) return "warning";
  return "success";
};

interface FeatureViewMetricsPanelProps {
  metrics: FeatureViewMetric[];
  isLoading: boolean;
  title: string;
  description?: string;
}

const FeatureViewMetricsPanel = ({
  metrics,
  isLoading,
  title,
  description,
}: FeatureViewMetricsPanelProps) => {
  if (isLoading) {
    return (
      <EuiPanel hasBorder>
        <EuiTitle size="xs">
          <h3>{title}</h3>
        </EuiTitle>
        <EuiSpacer size="m" />
        <EuiSkeletonText lines={4} />
      </EuiPanel>
    );
  }

  const latestByFV = new Map<string, FeatureViewMetric>();
  for (const m of metrics) {
    const existing = latestByFV.get(m.feature_view_name);
    if (!existing || m.metric_date > existing.metric_date) {
      latestByFV.set(m.feature_view_name, m);
    }
  }
  const latestMetrics = Array.from(latestByFV.values());

  const totalRows = latestMetrics.reduce(
    (sum, m) => sum + (m.total_row_count || 0),
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
  const healthyViews = latestMetrics.filter(
    (m) => m.avg_null_rate < 0.1,
  ).length;

  const columns: EuiBasicTableColumn<FeatureViewMetric>[] = [
    {
      field: "feature_view_name",
      name: "Feature View",
      sortable: true,
    },
    {
      field: "total_row_count",
      name: "Total Rows",
      sortable: true,
      render: (val: number) => (val || 0).toLocaleString(),
    },
    {
      field: "total_features",
      name: "Features",
      sortable: true,
      width: "80px",
    },
    {
      field: "features_with_nulls",
      name: "With Nulls",
      sortable: true,
      width: "90px",
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
        <h3>{title}</h3>
      </EuiTitle>
      {description && (
        <p style={{ color: "#69707D", fontSize: 13, marginTop: 4 }}>
          {description}
        </p>
      )}
      <EuiSpacer size="m" />

      <EuiFlexGroup>
        <EuiFlexItem>
          <EuiStat
            title={totalRows.toLocaleString()}
            description="Total Rows"
            titleSize="s"
            textAlign="center"
          />
        </EuiFlexItem>
        <EuiFlexItem>
          <EuiStat
            title={totalFeatures.toString()}
            description="Total Features"
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
        <EuiFlexItem>
          <EuiStat
            title={`${healthyViews}/${latestMetrics.length}`}
            description="Healthy Views"
            titleSize="s"
            titleColor="success"
            textAlign="center"
          />
        </EuiFlexItem>
      </EuiFlexGroup>

      <EuiSpacer size="m" />

      {latestMetrics.length > 0 && (
        <SortableTable items={latestMetrics} columns={columns} />
      )}
    </EuiPanel>
  );
};

const SortableTable = ({
  items,
  columns,
}: {
  items: FeatureViewMetric[];
  columns: EuiBasicTableColumn<FeatureViewMetric>[];
}) => {
  const [sortField, setSortField] = useState<string>("feature_view_name");
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

  const onTableChange = ({ sort }: Criteria<FeatureViewMetric>) => {
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
          field: sortField as keyof FeatureViewMetric,
          direction: sortDirection,
        },
      }}
      onChange={onTableChange}
      compressed
    />
  );
};

export default FeatureViewMetricsPanel;
