import React, { useContext, useEffect, useState } from "react";
import { useParams } from "react-router-dom";
import {
  EuiPanel,
  EuiTitle,
  EuiHorizontalRule,
  EuiText,
  EuiLoadingSpinner,
  EuiCallOut,
  EuiSpacer,
  EuiFlexGroup,
  EuiFlexItem,
  EuiStat,
  EuiBadge,
  EuiBasicTable,
  EuiBasicTableColumn,
  EuiProgress,
  EuiButton,
} from "@elastic/eui";
import RegistryPathContext from "../../contexts/RegistryPathContext";
import useLoadLabelView from "./useLoadLabelView";

interface QualityData {
  total_entities: number;
  feature_names: string[];
  distributions: Record<string, Record<string, number>>;
  coverage_pct: Record<string, number>;
  null_counts: Record<string, number>;
  labeler_stats: Record<string, number>;
  staleness_seconds: number | null;
  oldest_label_ts: string | null;
  newest_label_ts: string | null;
  labeler_field: string | null;
}

const formatStaleness = (seconds: number | null): string => {
  if (seconds === null) return "N/A";
  if (seconds < 60) return `${Math.round(seconds)}s ago`;
  if (seconds < 3600) return `${Math.round(seconds / 60)}m ago`;
  if (seconds < 86400) return `${Math.round(seconds / 3600)}h ago`;
  return `${Math.round(seconds / 86400)}d ago`;
};

const getStalenessColor = (seconds: number | null): string => {
  if (seconds === null) return "subdued";
  if (seconds < 3600) return "success";
  if (seconds < 86400) return "warning";
  return "danger";
};

const DistributionBar = ({
  distribution,
  label,
}: {
  distribution: Record<string, number>;
  label: string;
}) => {
  const entries = Object.entries(distribution).sort((a, b) => b[1] - a[1]);
  const total = entries.reduce((acc, [, count]) => acc + count, 0);
  const colors = [
    "#0569EA",
    "#00BFB3",
    "#F5A623",
    "#BD271E",
    "#6092C0",
    "#D36086",
    "#9170B8",
    "#CA8EAE",
  ];

  if (entries.length === 0) {
    return (
      <EuiText size="xs" color="subdued">
        No data
      </EuiText>
    );
  }

  return (
    <div>
      <EuiText size="xs">
        <strong>{label}</strong> ({total} values, {entries.length} unique)
      </EuiText>
      <EuiSpacer size="xs" />
      <div
        style={{
          display: "flex",
          height: "24px",
          borderRadius: "4px",
          overflow: "hidden",
          width: "100%",
        }}
      >
        {entries.slice(0, 8).map(([val, count], idx) => (
          <div
            key={val}
            style={{
              width: `${(count / total) * 100}%`,
              backgroundColor: colors[idx % colors.length],
              minWidth: "2px",
            }}
            title={`${val}: ${count} (${((count / total) * 100).toFixed(1)}%)`}
          />
        ))}
      </div>
      <EuiSpacer size="xs" />
      <EuiFlexGroup gutterSize="xs" wrap responsive={false}>
        {entries.slice(0, 6).map(([val, count], idx) => (
          <EuiFlexItem grow={false} key={val}>
            <EuiBadge
              color={colors[idx % colors.length]}
              style={{ fontSize: "10px" }}
            >
              {val.length > 12 ? val.slice(0, 12) + "\u2026" : val}: {count}
            </EuiBadge>
          </EuiFlexItem>
        ))}
        {entries.length > 6 && (
          <EuiFlexItem grow={false}>
            <EuiBadge color="hollow">+{entries.length - 6} more</EuiBadge>
          </EuiFlexItem>
        )}
      </EuiFlexGroup>
    </div>
  );
};

const QualityDashboardTab = () => {
  const { labelViewName } = useParams();
  const registryUrl = useContext(RegistryPathContext);
  const name = labelViewName || "";
  const { isLoading: lvLoading, isSuccess } = useLoadLabelView(name);

  const [quality, setQuality] = useState<QualityData | null>(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);

  const fetchQuality = async () => {
    setLoading(true);
    setError(null);
    try {
      const baseUrl = registryUrl?.replace(/\/$/, "") || "/api/v1";
      const response = await fetch(`${baseUrl}/label-quality`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ feature_view: name, limit: 500 }),
      });
      const result = await response.json();
      if (!response.ok) {
        const detail = result.detail;
        setError(
          typeof detail === "string"
            ? detail
            : Array.isArray(detail)
              ? detail.map((d: any) => d.msg || JSON.stringify(d)).join("; ")
              : "Failed to load quality metrics",
        );
      } else {
        setQuality(result);
      }
    } catch (e: any) {
      setError(e.message || "Network error");
    } finally {
      setLoading(false);
    }
  };

  useEffect(() => {
    if (isSuccess) {
      fetchQuality();
    }
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [isSuccess, name]);

  if (lvLoading) {
    return (
      <p>
        <EuiLoadingSpinner size="m" /> Loading...
      </p>
    );
  }

  if (loading) {
    return (
      <EuiPanel>
        <EuiFlexGroup alignItems="center" gutterSize="s">
          <EuiFlexItem grow={false}>
            <EuiLoadingSpinner size="m" />
          </EuiFlexItem>
          <EuiFlexItem>Computing label quality metrics...</EuiFlexItem>
        </EuiFlexGroup>
      </EuiPanel>
    );
  }

  if (error) {
    return (
      <EuiCallOut title="Quality Metrics Error" color="danger" iconType="alert">
        {error}
      </EuiCallOut>
    );
  }

  if (!quality) return null;

  const labelerColumns: EuiBasicTableColumn<{ name: string; count: number }>[] =
    [
      { field: "name", name: "Labeler", sortable: true },
      {
        field: "count",
        name: "Labels Submitted",
        sortable: true,
        render: (count: number) => <EuiBadge color="primary">{count}</EuiBadge>,
      },
      {
        field: "count",
        name: "Share",
        render: (count: number) => {
          const total = Object.values(quality.labeler_stats).reduce(
            (a, b) => a + b,
            0,
          );
          return `${((count / total) * 100).toFixed(1)}%`;
        },
      },
    ];

  const labelerData = Object.entries(quality.labeler_stats)
    .map(([name, count]) => ({ name, count }))
    .sort((a, b) => b.count - a.count);

  return (
    <React.Fragment>
      {/* Time Range + Refresh at top */}
      <EuiPanel color="subdued">
        <EuiFlexGroup alignItems="center">
          <EuiFlexItem>
            <EuiFlexGroup gutterSize="l" responsive={false}>
              <EuiFlexItem grow={false}>
                <EuiText size="s">
                  <strong>Oldest label:</strong>{" "}
                  {quality.oldest_label_ts
                    ? new Date(quality.oldest_label_ts).toLocaleString()
                    : "N/A"}
                </EuiText>
              </EuiFlexItem>
              <EuiFlexItem grow={false}>
                <EuiText size="s">
                  <strong>Newest label:</strong>{" "}
                  {quality.newest_label_ts
                    ? new Date(quality.newest_label_ts).toLocaleString()
                    : "N/A"}
                </EuiText>
              </EuiFlexItem>
            </EuiFlexGroup>
          </EuiFlexItem>
          <EuiFlexItem grow={false}>
            <EuiButton size="s" onClick={fetchQuality} iconType="refresh">
              Refresh Metrics
            </EuiButton>
          </EuiFlexItem>
        </EuiFlexGroup>
      </EuiPanel>

      <EuiSpacer size="l" />

      {/* Summary Stats */}
      <EuiFlexGroup>
        <EuiFlexItem>
          <EuiPanel>
            <EuiStat
              title={quality.total_entities.toLocaleString()}
              description="Total Records Labeled"
              titleColor="primary"
              titleSize="l"
            />
          </EuiPanel>
        </EuiFlexItem>
        <EuiFlexItem>
          <EuiPanel>
            <EuiStat
              title={Object.keys(quality.labeler_stats).length.toString()}
              description="Active Labelers"
              titleColor="accent"
              titleSize="l"
            />
          </EuiPanel>
        </EuiFlexItem>
        <EuiFlexItem>
          <EuiPanel>
            <EuiStat
              title={formatStaleness(quality.staleness_seconds)}
              description="Last Label"
              titleColor={getStalenessColor(quality.staleness_seconds)}
              titleSize="l"
            />
          </EuiPanel>
        </EuiFlexItem>
        <EuiFlexItem>
          <EuiPanel>
            <EuiStat
              title={quality.feature_names.length.toString()}
              description="Label Fields"
              titleColor="subdued"
              titleSize="l"
            />
          </EuiPanel>
        </EuiFlexItem>
      </EuiFlexGroup>

      <EuiSpacer size="l" />

      {/* Coverage */}
      <EuiPanel>
        <EuiTitle size="xs">
          <h3>Field Coverage</h3>
        </EuiTitle>
        <EuiText size="xs" color="subdued">
          Percentage of records with non-null values for each label field
        </EuiText>
        <EuiSpacer size="m" />
        {quality.feature_names.map((fn) => (
          <React.Fragment key={fn}>
            <EuiFlexGroup alignItems="center" gutterSize="s">
              <EuiFlexItem grow={false} style={{ width: "150px" }}>
                <EuiText size="s">
                  <strong>{fn}</strong>
                </EuiText>
              </EuiFlexItem>
              <EuiFlexItem>
                <EuiProgress
                  value={quality.coverage_pct[fn] || 0}
                  max={100}
                  size="l"
                  color={
                    (quality.coverage_pct[fn] || 0) > 80
                      ? "success"
                      : (quality.coverage_pct[fn] || 0) > 50
                        ? "warning"
                        : "danger"
                  }
                  label={`${(quality.coverage_pct[fn] || 0).toFixed(1)}%`}
                />
              </EuiFlexItem>
              <EuiFlexItem grow={false}>
                <EuiBadge color="hollow">
                  {quality.null_counts[fn] || 0} nulls
                </EuiBadge>
              </EuiFlexItem>
            </EuiFlexGroup>
            <EuiSpacer size="s" />
          </React.Fragment>
        ))}
      </EuiPanel>

      <EuiSpacer size="l" />

      {/* Distributions */}
      <EuiPanel>
        <EuiTitle size="xs">
          <h3>Value Distributions</h3>
        </EuiTitle>
        <EuiText size="xs" color="subdued">
          Distribution of label values across all records
        </EuiText>
        <EuiSpacer size="m" />
        {quality.feature_names.map((fn) => (
          <React.Fragment key={fn}>
            <DistributionBar
              distribution={quality.distributions[fn] || {}}
              label={fn}
            />
            <EuiSpacer size="m" />
            <EuiHorizontalRule margin="s" />
          </React.Fragment>
        ))}
      </EuiPanel>

      <EuiSpacer size="l" />

      {/* Per-Labeler Stats */}
      {labelerData.length > 0 && (
        <EuiPanel>
          <EuiFlexGroup alignItems="center">
            <EuiFlexItem>
              <EuiTitle size="xs">
                <h3>Per-Labeler Statistics</h3>
              </EuiTitle>
            </EuiFlexItem>
            {quality.labeler_field && (
              <EuiFlexItem grow={false}>
                <EuiBadge color="accent">
                  Tracked via: {quality.labeler_field}
                </EuiBadge>
              </EuiFlexItem>
            )}
          </EuiFlexGroup>
          <EuiSpacer size="m" />
          <EuiBasicTable items={labelerData} columns={labelerColumns} />
        </EuiPanel>
      )}
    </React.Fragment>
  );
};

export default QualityDashboardTab;
