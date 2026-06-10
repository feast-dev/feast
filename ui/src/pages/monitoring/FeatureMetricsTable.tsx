import React, { useState, useMemo, useEffect } from "react";
import {
  EuiBasicTable,
  EuiBasicTableColumn,
  EuiBadge,
  EuiButtonIcon,
  EuiDescriptionList,
  EuiFlexGroup,
  EuiFlexItem,
  EuiHealth,
  EuiLink,
  EuiPopover,
  EuiProgress,
  EuiTitle,
  EuiToolTip,
  Criteria,
} from "@elastic/eui";
import type {
  FeatureMetric,
  NumericHistogram,
  CategoricalHistogram,
} from "../../queries/useMonitoringApi";

const healthColor = (nullRate: number): string => {
  if (nullRate >= 0.5) return "danger";
  if (nullRate >= 0.1) return "warning";
  return "success";
};

const healthLabel = (nullRate: number): string => {
  if (nullRate >= 0.5) return "High null rate";
  if (nullRate >= 0.1) return "Moderate null rate";
  return "Healthy";
};

const formatNum = (val: number | null, decimals = 2): string => {
  if (val === null || val === undefined) return "—";
  if (Number.isInteger(val)) return val.toLocaleString();
  return val.toFixed(decimals);
};

const formatFreshness = (computedAt: string | null): string => {
  if (!computedAt) return "—";
  const diff = Date.now() - new Date(computedAt).getTime();
  const mins = Math.floor(diff / 60_000);
  if (mins < 1) return "just now";
  if (mins < 60) return `${mins}m ago`;
  const hrs = Math.floor(mins / 60);
  if (hrs < 24) return `${hrs}h ago`;
  const days = Math.floor(hrs / 24);
  if (days < 30) return `${days}d ago`;
  return `${Math.floor(days / 30)}mo ago`;
};

const freshnessColor = (computedAt: string | null): string => {
  if (!computedAt) return "subdued";
  const hrs = (Date.now() - new Date(computedAt).getTime()) / 3_600_000;
  if (hrs < 24) return "success";
  if (hrs < 72) return "warning";
  return "danger";
};

const MiniHistogram = ({ metric }: { metric: FeatureMetric }) => {
  if (!metric.histogram) return <span style={{ color: "#98A2B3" }}>—</span>;

  const width = 120;
  const height = 28;

  if (metric.feature_type === "numeric") {
    const hist = metric.histogram as NumericHistogram;
    const maxCount = Math.max(...hist.counts, 1);
    const barW = Math.max(Math.floor(width / hist.counts.length) - 1, 2);

    return (
      <EuiToolTip content="Click feature name for full distribution">
        <svg width={width} height={height} style={{ display: "block" }}>
          {hist.counts.map((count, i) => {
            const h = (count / maxCount) * (height - 2);
            return (
              <rect
                key={i}
                x={i * (barW + 1)}
                y={height - h}
                width={barW}
                height={Math.max(h, 1)}
                fill="#006BB4"
                opacity={0.8}
                rx={1}
              />
            );
          })}
        </svg>
      </EuiToolTip>
    );
  }

  const hist = metric.histogram as CategoricalHistogram;
  const maxCount = Math.max(...hist.values.map((v) => v.count), 1);
  const barW = Math.max(
    Math.floor(width / Math.min(hist.values.length, 10)) - 1,
    6,
  );

  return (
    <EuiToolTip content="Click feature name for full distribution">
      <svg width={width} height={height} style={{ display: "block" }}>
        {hist.values.slice(0, 10).map((v, i) => {
          const h = (v.count / maxCount) * (height - 2);
          return (
            <rect
              key={i}
              x={i * (barW + 1)}
              y={height - h}
              width={barW}
              height={Math.max(h, 1)}
              fill="#E7664C"
              opacity={0.8}
              rx={1}
            />
          );
        })}
      </svg>
    </EuiToolTip>
  );
};

interface FeatureMetricsTableProps {
  metrics: FeatureMetric[];
  isLoading: boolean;
  onFeatureClick: (fvName: string, featureName: string) => void;
}

const PAGE_SIZE_OPTIONS = [10, 20, 50];

const FeatureMetricsTable = ({
  metrics,
  isLoading,
  onFeatureClick,
}: FeatureMetricsTableProps) => {
  const [sortField, setSortField] =
    useState<keyof FeatureMetric>("feature_view_name");
  const [sortDirection, setSortDirection] = useState<"asc" | "desc">("asc");
  const [pageIndex, setPageIndex] = useState(0);
  const [pageSize, setPageSize] = useState(20);

  useEffect(() => {
    setPageIndex(0);
  }, [metrics]);

  const latestMetrics = useMemo(() => {
    const byKey = new Map<string, FeatureMetric>();
    for (const m of metrics) {
      const key = `${m.feature_view_name}::${m.feature_name}`;
      const existing = byKey.get(key);
      if (!existing) {
        byKey.set(key, m);
      } else {
        const preferNew =
          m.row_count > 0 && existing.row_count === 0
            ? true
            : existing.row_count > 0 && m.row_count === 0
              ? false
              : m.metric_date > existing.metric_date;
        if (preferNew) byKey.set(key, m);
      }
    }
    return Array.from(byKey.values());
  }, [metrics]);

  const sortedItems = useMemo(() => {
    return [...latestMetrics].sort((a, b) => {
      const aVal = a[sortField];
      const bVal = b[sortField];
      if (aVal == null && bVal == null) return 0;
      if (aVal == null) return 1;
      if (bVal == null) return -1;
      if (aVal < bVal) return sortDirection === "asc" ? -1 : 1;
      if (aVal > bVal) return sortDirection === "asc" ? 1 : -1;
      return 0;
    });
  }, [latestMetrics, sortField, sortDirection]);

  const pageOfItems = useMemo(() => {
    const start = pageIndex * pageSize;
    return sortedItems.slice(start, start + pageSize);
  }, [sortedItems, pageIndex, pageSize]);

  const pagination = useMemo(
    () => ({
      pageIndex,
      pageSize,
      totalItemCount: sortedItems.length,
      pageSizeOptions: PAGE_SIZE_OPTIONS,
    }),
    [pageIndex, pageSize, sortedItems.length],
  );

  const onTableChange = ({ sort, page }: Criteria<FeatureMetric>) => {
    if (sort) {
      setSortField(sort.field as keyof FeatureMetric);
      setSortDirection(sort.direction);
    }
    if (page) {
      setPageIndex(page.index);
      setPageSize(page.size);
    }
  };

  const [isLegendOpen, setIsLegendOpen] = useState(false);

  const columnLegend = [
    {
      title: "Feature",
      description:
        "Name of the individual feature. Click to view full distribution and detailed statistics.",
    },
    {
      title: "Feature View",
      description:
        "The feature view this feature belongs to — a logical grouping of related features sharing the same data source.",
    },
    {
      title: "Type",
      description:
        "Data type: numeric (continuous/discrete numbers) or categorical (strings/labels).",
    },
    {
      title: "Distribution",
      description:
        "Compact histogram showing the value distribution. Blue bars = numeric, orange bars = categorical.",
    },
    {
      title: "Rows",
      description:
        "Total number of rows (data points) observed for this feature in the computed time window.",
    },
    {
      title: "Null Rate",
      description:
        "Percentage of rows with missing (null) values. Shown as a progress bar colored by severity.",
    },
    {
      title: "Health",
      description:
        "Data quality indicator based on null rate: Healthy (< 10%), Moderate (10–49%), High (>= 50%).",
    },
    {
      title: "Mean",
      description:
        "Arithmetic mean of the feature values. Only shown for numeric features.",
    },
    {
      title: "Std Dev",
      description:
        "Standard deviation — measures how spread out the values are from the mean. Only for numeric features.",
    },
    {
      title: "Freshness",
      description:
        "Recency of the underlying data. Green (< 24h old), Yellow (24–72h), Red (> 72h). Hover for the data date.",
    },
    {
      title: "Source",
      description:
        "Data source type used for metric computation (e.g. batch, stream).",
    },
  ];

  const columns: EuiBasicTableColumn<FeatureMetric>[] = [
    {
      field: "feature_name",
      name: "Feature",
      sortable: true,
      render: (name: string, item: FeatureMetric) => (
        <EuiLink onClick={() => onFeatureClick(item.feature_view_name, name)}>
          {name}
        </EuiLink>
      ),
    },
    {
      field: "feature_view_name",
      name: "Feature View",
      sortable: true,
    },
    {
      field: "feature_type",
      name: "Type",
      sortable: true,
      width: "100px",
      render: (type: string) => (
        <EuiBadge color={type === "numeric" ? "primary" : "accent"}>
          {type}
        </EuiBadge>
      ),
    },
    {
      name: "Distribution",
      width: "140px",
      render: (item: FeatureMetric) => <MiniHistogram metric={item} />,
    },
    {
      field: "row_count",
      name: "Rows",
      sortable: true,
      width: "90px",
      render: (val: number) => formatNum(val, 0),
    },
    {
      field: "null_rate",
      name: "Null Rate",
      sortable: true,
      width: "150px",
      render: (val: number) => (
        <div style={{ display: "flex", alignItems: "center", gap: 8 }}>
          <EuiProgress
            value={val * 100}
            max={100}
            color={healthColor(val) as any}
            size="m"
            style={{ width: 60 }}
          />
          <span>{(val * 100).toFixed(1)}%</span>
        </div>
      ),
    },
    {
      field: "null_rate",
      name: "Health",
      width: "130px",
      render: (val: number) => (
        <EuiHealth color={healthColor(val)}>{healthLabel(val)}</EuiHealth>
      ),
    },
    {
      field: "mean",
      name: "Mean",
      sortable: true,
      width: "100px",
      render: (val: number | null) => formatNum(val),
    },
    {
      field: "stddev",
      name: "Std Dev",
      sortable: true,
      width: "100px",
      render: (val: number | null) => formatNum(val),
    },
    {
      field: "metric_date",
      name: "Freshness",
      sortable: true,
      width: "110px",
      render: (val: string) => (
        <EuiToolTip content={val ? `Data from: ${val}` : "Unknown"}>
          <EuiHealth color={freshnessColor(val)}>
            {formatFreshness(val)}
          </EuiHealth>
        </EuiToolTip>
      ),
    },
    {
      field: "data_source_type",
      name: "Source",
      width: "80px",
      render: (val: string) => <EuiBadge color="hollow">{val}</EuiBadge>,
    },
  ];

  return (
    <>
      <EuiFlexGroup justifyContent="flexEnd" gutterSize="none">
        <EuiFlexItem grow={false}>
          <EuiPopover
            button={
              <EuiButtonIcon
                iconType="questionInCircle"
                aria-label="Column legend"
                color="text"
                onClick={() => setIsLegendOpen(!isLegendOpen)}
              />
            }
            isOpen={isLegendOpen}
            closePopover={() => setIsLegendOpen(false)}
            anchorPosition="downRight"
            panelPaddingSize="m"
            panelStyle={{ maxWidth: 420 }}
          >
            <EuiTitle size="xxs">
              <h4>Column Legend</h4>
            </EuiTitle>
            <EuiDescriptionList
              type="column"
              compressed
              listItems={columnLegend}
              style={{ marginTop: 8 }}
              titleProps={{ style: { fontWeight: 600, width: 100 } }}
            />
          </EuiPopover>
        </EuiFlexItem>
      </EuiFlexGroup>
      <EuiBasicTable
        items={pageOfItems}
        columns={columns}
        loading={isLoading}
        pagination={pagination}
        sorting={{
          sort: {
            field: sortField,
            direction: sortDirection,
          },
        }}
        onChange={onTableChange}
        rowProps={(item: FeatureMetric) => ({
          "data-test-subj": `row-${item.feature_name}`,
        })}
        noItemsMessage={
          isLoading
            ? "Loading metrics..."
            : "No metrics found. Run a monitoring compute job to generate metrics."
        }
      />
    </>
  );
};

export default FeatureMetricsTable;
