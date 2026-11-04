import React, { useState, useMemo } from "react";
import {
  EuiPanel,
  EuiTitle,
  EuiSpacer,
  EuiFlexGroup,
  EuiFlexItem,
  EuiSuperSelect,
} from "@elastic/eui";
import type {
  FeatureMetric,
  CategoricalHistogram,
} from "../../../queries/useMonitoringApi";

const COLORS = [
  "#006BB4",
  "#54B399",
  "#E7664C",
  "#9170B8",
  "#D36086",
  "#6092C0",
  "#D6BF57",
  "#B9A888",
];

const RANGE_OPTIONS = [
  { value: "24h", inputDisplay: "Last 24 hours" },
  { value: "7d", inputDisplay: "Last 7 days" },
  { value: "30d", inputDisplay: "Last 30 days" },
  { value: "90d", inputDisplay: "Last 90 days" },
  { value: "all", inputDisplay: "All time" },
];

const rangeToMs: Record<string, number> = {
  "24h": 24 * 3600_000,
  "7d": 7 * 86400_000,
  "30d": 30 * 86400_000,
  "90d": 90 * 86400_000,
  all: Infinity,
};

interface ChartDims {
  width: number;
  height: number;
  padLeft: number;
  padRight: number;
  padTop: number;
  padBottom: number;
}

const DIMS: ChartDims = {
  width: 860,
  height: 220,
  padLeft: 60,
  padRight: 20,
  padTop: 10,
  padBottom: 30,
};

const formatDate = (d: string): string => {
  const dt = new Date(d);
  const mm = String(dt.getMonth() + 1).padStart(2, "0");
  const dd = String(dt.getDate()).padStart(2, "0");
  const hh = String(dt.getHours()).padStart(2, "0");
  const mi = String(dt.getMinutes()).padStart(2, "0");
  return `${mm}-${dd} ${hh}:${mi}`;
};

const formatAxisVal = (v: number): string => {
  if (v === 0) return "0";
  const abs = Math.abs(v);
  if (abs >= 1_000_000) return (v / 1_000_000).toFixed(1) + "M";
  if (abs >= 1_000) return (v / 1_000).toFixed(1) + "K";
  if (abs >= 1) return v.toFixed(1);
  return (v * 100).toFixed(1) + "%";
};

const niceYTicks = (min: number, max: number, count = 5): number[] => {
  if (max === min) return [min];
  const step = (max - min) / (count - 1);
  return Array.from({ length: count }, (_, i) => min + step * i);
};

interface LineSeriesData {
  label: string;
  color: string;
  dashArray?: string;
  points: { x: number; y: number; date: string; value: number }[];
}

const renderMultiLineChart = (
  series: LineSeriesData[],
  dims: ChartDims,
  yLabel: string,
  yFormatter: (v: number) => string = formatAxisVal,
) => {
  const allPoints = series.flatMap((s) => s.points);
  if (allPoints.length === 0) return null;

  const xMin = Math.min(...allPoints.map((p) => p.x));
  const xMax = Math.max(...allPoints.map((p) => p.x));
  const yMin = Math.min(...allPoints.map((p) => p.value), 0);
  const yMax = Math.max(...allPoints.map((p) => p.value), 0.01);

  const plotW = dims.width - dims.padLeft - dims.padRight;
  const plotH = dims.height - dims.padTop - dims.padBottom;

  const scaleX = (x: number) =>
    xMax === xMin
      ? dims.padLeft + plotW / 2
      : dims.padLeft + ((x - xMin) / (xMax - xMin)) * plotW;
  const scaleY = (v: number) =>
    dims.padTop + plotH - ((v - yMin) / (yMax - yMin || 1)) * plotH;

  const yTicks = niceYTicks(yMin, yMax);
  const xDates = allPoints
    .map((p) => ({ x: p.x, date: p.date }))
    .filter((v, i, arr) => arr.findIndex((a) => a.date === v.date) === i)
    .sort((a, b) => a.x - b.x);

  const maxXLabels = Math.floor(plotW / 80);
  const xStep = Math.max(1, Math.ceil(xDates.length / maxXLabels));
  const xLabels = xDates.filter((_, i) => i % xStep === 0);

  return (
    <svg width={dims.width} height={dims.height} style={{ display: "block" }}>
      {/* Y axis grid + labels */}
      {yTicks.map((v, i) => {
        const y = scaleY(v);
        return (
          <g key={`y-${i}`}>
            <line
              x1={dims.padLeft}
              y1={y}
              x2={dims.width - dims.padRight}
              y2={y}
              stroke="#E0E5EE"
              strokeDasharray="3,3"
            />
            <text
              x={dims.padLeft - 6}
              y={y + 4}
              textAnchor="end"
              fontSize={10}
              fill="#69707D"
            >
              {yFormatter(v)}
            </text>
          </g>
        );
      })}

      {/* Y axis label */}
      <text
        x={14}
        y={dims.padTop + plotH / 2}
        textAnchor="middle"
        fontSize={11}
        fill="#E7664C"
        fontWeight={600}
        transform={`rotate(-90, 14, ${dims.padTop + plotH / 2})`}
      >
        {yLabel}
      </text>

      {/* X axis labels */}
      {xLabels.map((xl, i) => (
        <text
          key={`x-${i}`}
          x={scaleX(xl.x)}
          y={dims.height - 4}
          textAnchor="middle"
          fontSize={10}
          fill="#69707D"
        >
          {formatDate(xl.date)}
        </text>
      ))}

      {/* Lines */}
      {series.map((s, si) => {
        if (s.points.length === 0) return null;
        const sorted = [...s.points].sort((a, b) => a.x - b.x);
        const pathD = sorted
          .map(
            (p, i) =>
              `${i === 0 ? "M" : "L"} ${scaleX(p.x)} ${scaleY(p.value)}`,
          )
          .join(" ");
        return (
          <g key={si}>
            <path
              d={pathD}
              fill="none"
              stroke={s.color}
              strokeWidth={2}
              strokeDasharray={s.dashArray}
            />
            {sorted.map((p, i) => (
              <circle
                key={i}
                cx={scaleX(p.x)}
                cy={scaleY(p.value)}
                r={s.dashArray ? 4 : 3}
                fill={s.color}
                stroke={s.dashArray ? "#fff" : undefined}
                strokeWidth={s.dashArray ? 1 : undefined}
              />
            ))}
          </g>
        );
      })}
    </svg>
  );
};

const renderAreaChart = (
  points: { x: number; value: number; date: string }[],
  dims: ChartDims,
  yLabel: string,
  lineColor: string,
  fillColor: string,
) => {
  if (points.length === 0) return null;

  const sorted = [...points].sort((a, b) => a.x - b.x);
  const xMin = Math.min(...sorted.map((p) => p.x));
  const xMax = Math.max(...sorted.map((p) => p.x));
  const yMin = 0;
  const yMax = Math.max(...sorted.map((p) => p.value), 0.01);

  const plotW = dims.width - dims.padLeft - dims.padRight;
  const plotH = dims.height - dims.padTop - dims.padBottom;

  const scaleX = (x: number) =>
    xMax === xMin
      ? dims.padLeft + plotW / 2
      : dims.padLeft + ((x - xMin) / (xMax - xMin)) * plotW;
  const scaleY = (v: number) =>
    dims.padTop + plotH - ((v - yMin) / (yMax - yMin || 1)) * plotH;

  const yTicks = niceYTicks(yMin, yMax);
  const baseline = scaleY(0);

  const areaPath =
    `M ${scaleX(sorted[0].x)} ${baseline} ` +
    sorted.map((p) => `L ${scaleX(p.x)} ${scaleY(p.value)}`).join(" ") +
    ` L ${scaleX(sorted[sorted.length - 1].x)} ${baseline} Z`;

  const linePath = sorted
    .map((p, i) => `${i === 0 ? "M" : "L"} ${scaleX(p.x)} ${scaleY(p.value)}`)
    .join(" ");

  const maxXLabels = Math.floor(plotW / 80);
  const xStep = Math.max(1, Math.ceil(sorted.length / maxXLabels));
  const xLabels = sorted.filter((_, i) => i % xStep === 0);

  return (
    <svg width={dims.width} height={dims.height} style={{ display: "block" }}>
      {yTicks.map((v, i) => {
        const y = scaleY(v);
        return (
          <g key={`y-${i}`}>
            <line
              x1={dims.padLeft}
              y1={y}
              x2={dims.width - dims.padRight}
              y2={y}
              stroke="#E0E5EE"
              strokeDasharray="3,3"
            />
            <text
              x={dims.padLeft - 6}
              y={y + 4}
              textAnchor="end"
              fontSize={10}
              fill="#69707D"
            >
              {(v * 100).toFixed(0)}%
            </text>
          </g>
        );
      })}

      <text
        x={14}
        y={dims.padTop + plotH / 2}
        textAnchor="middle"
        fontSize={11}
        fill="#E7664C"
        fontWeight={600}
        transform={`rotate(-90, 14, ${dims.padTop + plotH / 2})`}
      >
        {yLabel}
      </text>

      {xLabels.map((xl, i) => (
        <text
          key={`x-${i}`}
          x={scaleX(xl.x)}
          y={dims.height - 4}
          textAnchor="middle"
          fontSize={10}
          fill="#69707D"
        >
          {formatDate(xl.date)}
        </text>
      ))}

      <path d={areaPath} fill={fillColor} />
      <path d={linePath} fill="none" stroke={lineColor} strokeWidth={2} />
    </svg>
  );
};

const Legend = ({
  items,
}: {
  items: { label: string; color: string; dashed?: boolean }[];
}) => (
  <div
    style={{
      display: "flex",
      flexWrap: "wrap",
      gap: 16,
      justifyContent: "center",
      marginTop: 8,
    }}
  >
    {items.map((item, i) => (
      <div key={i} style={{ display: "flex", alignItems: "center", gap: 4 }}>
        {item.dashed ? (
          <svg width={16} height={12}>
            <line
              x1={0}
              y1={6}
              x2={16}
              y2={6}
              stroke={item.color}
              strokeWidth={2}
              strokeDasharray="4,2"
            />
          </svg>
        ) : (
          <div
            style={{
              width: 12,
              height: 12,
              backgroundColor: item.color,
              borderRadius: 2,
            }}
          />
        )}
        <span style={{ fontSize: 12, color: "#343741" }}>{item.label}</span>
      </div>
    ))}
  </div>
);

interface TimeSeriesAnalysisProps {
  metrics: FeatureMetric[];
  featureType: string;
}

const TimeSeriesAnalysis = ({
  metrics,
  featureType,
}: TimeSeriesAnalysisProps) => {
  const [range, setRange] = useState("all");

  const filteredMetrics = useMemo(() => {
    if (range === "all") return metrics;
    const cutoff = Date.now() - rangeToMs[range];
    return metrics.filter((m) => new Date(m.metric_date).getTime() >= cutoff);
  }, [metrics, range]);

  const sorted = useMemo(
    () =>
      [...filteredMetrics]
        .filter((m) => m.row_count > 0)
        .sort((a, b) => a.metric_date.localeCompare(b.metric_date)),
    [filteredMetrics],
  );

  const toX = (m: FeatureMetric) => new Date(m.metric_date).getTime();
  const isNumeric = featureType === "numeric";
  const hasData = sorted.length >= 1;

  return (
    <EuiPanel hasBorder>
      <EuiFlexGroup justifyContent="spaceBetween" alignItems="center">
        <EuiFlexItem grow={false}>
          <EuiTitle size="xs">
            <h3>Time-Series Analysis</h3>
          </EuiTitle>
          <p style={{ color: "#69707D", fontSize: 12, marginTop: 2 }}>
            Historical trends for central aggregates and quality signals.
          </p>
        </EuiFlexItem>
        <EuiFlexItem grow={false} style={{ minWidth: 160 }}>
          <EuiSuperSelect
            options={RANGE_OPTIONS}
            valueOfSelected={range}
            onChange={setRange}
            compressed
          />
        </EuiFlexItem>
      </EuiFlexGroup>

      <EuiSpacer size="m" />

      {!hasData ? (
        <p
          style={{
            color: "#69707D",
            fontSize: 13,
            textAlign: "center",
            padding: 24,
          }}
        >
          No data points available for the selected time range. Try a wider
          range.
        </p>
      ) : isNumeric ? (
        <NumericTimeSeries metrics={sorted} toX={toX} />
      ) : (
        <CategoricalTimeSeries metrics={sorted} toX={toX} />
      )}
    </EuiPanel>
  );
};

const NumericTimeSeries = ({
  metrics,
  toX,
}: {
  metrics: FeatureMetric[];
  toX: (m: FeatureMetric) => number;
}) => {
  const driftSeries: LineSeriesData[] = useMemo(() => {
    const build = (
      label: string,
      color: string,
      accessor: (m: FeatureMetric) => number | null,
      dashArray?: string,
    ): LineSeriesData => ({
      label,
      color,
      dashArray,
      points: metrics
        .filter((m) => accessor(m) !== null)
        .map((m) => ({
          x: toX(m),
          y: 0,
          value: accessor(m)!,
          date: m.metric_date,
        })),
    });
    return [
      build("Mean", COLORS[0], (m) => m.mean, "6,3"),
      build("P50", COLORS[1], (m) => m.p50),
      build("P95", COLORS[2], (m) => m.p95),
    ];
  }, [metrics, toX]);

  const nullPoints = useMemo(
    () =>
      metrics.map((m) => ({
        x: toX(m),
        value: m.null_rate,
        date: m.metric_date,
      })),
    [metrics, toX],
  );

  return (
    <>
      <h4 style={{ fontSize: 14, fontWeight: 600, marginBottom: 4 }}>
        Aggregate Metrics Drift (Mean/P50/P95)
      </h4>
      <div style={{ overflowX: "auto" }}>
        {renderMultiLineChart(driftSeries, DIMS, "Metric Value")}
      </div>
      <Legend
        items={[
          { label: "Mean", color: COLORS[0], dashed: true },
          { label: "P50", color: COLORS[1] },
          { label: "P95", color: COLORS[2] },
        ]}
      />

      <EuiSpacer size="l" />

      <h4 style={{ fontSize: 14, fontWeight: 600, marginBottom: 4 }}>
        Null Rate Evolution (%)
      </h4>
      <div style={{ overflowX: "auto" }}>
        {renderAreaChart(
          nullPoints,
          { ...DIMS, height: 160 },
          "Percentage (%)",
          "#BD271E",
          "rgba(189, 39, 30, 0.15)",
        )}
      </div>
    </>
  );
};

const CategoricalTimeSeries = ({
  metrics,
  toX,
}: {
  metrics: FeatureMetric[];
  toX: (m: FeatureMetric) => number;
}) => {
  const { cardinalitySeries, shareSeries, topCategories } = useMemo(() => {
    const catCounts = new Map<string, number>();
    for (const m of metrics) {
      const hist = m.histogram as CategoricalHistogram | null;
      if (!hist) continue;
      for (const v of hist.values) {
        catCounts.set(v.value, (catCounts.get(v.value) || 0) + v.count);
      }
    }
    const topCats = Array.from(catCounts.entries())
      .sort((a, b) => b[1] - a[1])
      .slice(0, 5)
      .map(([v]) => v);

    const cardSeries: LineSeriesData[] = [
      {
        label: "Cardinality",
        color: COLORS[0],
        points: metrics
          .filter((m) => m.histogram)
          .map((m) => ({
            x: toX(m),
            y: 0,
            value: (m.histogram as CategoricalHistogram).unique_count || 0,
            date: m.metric_date,
          })),
      },
      ...topCats.map((cat, i) => ({
        label: cat,
        color: COLORS[(i + 1) % COLORS.length],
        points: metrics
          .filter((m) => m.histogram)
          .map((m) => {
            const hist = m.histogram as CategoricalHistogram;
            const entry = hist.values.find((v) => v.value === cat);
            return {
              x: toX(m),
              y: 0,
              value: entry?.count || 0,
              date: m.metric_date,
            };
          }),
      })),
    ];

    const shrSeries: LineSeriesData[] = topCats.map((cat, i) => ({
      label: cat,
      color: COLORS[(i + 1) % COLORS.length],
      points: metrics
        .filter((m) => m.histogram && m.row_count > 0)
        .map((m) => {
          const hist = m.histogram as CategoricalHistogram;
          const entry = hist.values.find((v) => v.value === cat);
          return {
            x: toX(m),
            y: 0,
            value: ((entry?.count || 0) / m.row_count) * 100,
            date: m.metric_date,
          };
        }),
    }));

    return {
      cardinalitySeries: cardSeries,
      shareSeries: shrSeries,
      topCategories: topCats,
    };
  }, [metrics, toX]);

  const nullPoints = useMemo(
    () =>
      metrics.map((m) => ({
        x: toX(m),
        value: m.null_rate,
        date: m.metric_date,
      })),
    [metrics, toX],
  );

  const shareYFormatter = (v: number) => `${v.toFixed(0)}%`;

  return (
    <>
      <h4 style={{ fontSize: 14, fontWeight: 600, marginBottom: 4 }}>
        Cardinality over time
      </h4>
      <div style={{ overflowX: "auto" }}>
        {renderMultiLineChart(cardinalitySeries, DIMS, "Count")}
      </div>
      <Legend
        items={cardinalitySeries.map((s) => ({
          label: s.label,
          color: s.color,
        }))}
      />

      <EuiSpacer size="l" />

      <h4 style={{ fontSize: 14, fontWeight: 600, marginBottom: 4 }}>
        Top category share over time (%)
      </h4>
      <div style={{ overflowX: "auto" }}>
        {renderMultiLineChart(
          shareSeries,
          DIMS,
          "Percentage (%)",
          shareYFormatter,
        )}
      </div>
      <Legend
        items={topCategories.map((cat, i) => ({
          label: cat,
          color: COLORS[(i + 1) % COLORS.length],
        }))}
      />

      <EuiSpacer size="l" />

      <h4 style={{ fontSize: 14, fontWeight: 600, marginBottom: 4 }}>
        Null Rate Evolution (%)
      </h4>
      <div style={{ overflowX: "auto" }}>
        {renderAreaChart(
          nullPoints,
          { ...DIMS, height: 160 },
          "Percentage (%)",
          "#BD271E",
          "rgba(189, 39, 30, 0.15)",
        )}
      </div>
    </>
  );
};

export default TimeSeriesAnalysis;
