import React, { useState, useMemo, useEffect } from "react";
import {
  EuiPanel,
  EuiTitle,
  EuiSpacer,
  EuiFlexGroup,
  EuiFlexItem,
  EuiSuperSelect,
  EuiButtonIcon,
  EuiButtonEmpty,
} from "@elastic/eui";
import type {
  FeatureMetric,
  CategoricalHistogram,
} from "../../../queries/useMonitoringApi";
import {
  ChartTooltip,
  PlotOverlay,
  ZoomScroller,
  inDomain,
  useClipId,
  usePlotPointer,
  useZoom,
} from "./chartZoom";
import type { PlotBox, XDomain } from "./chartZoom";

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

const DAY_MS = 86400_000;
// Point markers are hidden when a series shows more points than this.
const MAX_MARKERS = 45;
interface ChartInteraction {
  // Visible window, and the full range of the data.
  domain: XDomain;
  fullDomain: XDomain;
  hoverX: number | null;
  setHoverX: (x: number | null) => void;
  zoomTo: (domain: XDomain) => void;
  // Move the window so it starts at `start`, keeping its width.
  panTo: (start: number) => void;
  resetZoom: () => void;
}

const formatDate = (d: string): string => {
  // Metric dates are calendar days. Show them as they are, so the browser's
  // time zone does not shift them to another day or add a time.
  if (/^\d{4}-\d{2}-\d{2}$/.test(d)) return d.slice(5);
  const dt = new Date(d);
  const mm = String(dt.getMonth() + 1).padStart(2, "0");
  const dd = String(dt.getDate()).padStart(2, "0");
  const hh = String(dt.getHours()).padStart(2, "0");
  const mi = String(dt.getMinutes()).padStart(2, "0");
  return `${mm}-${dd} ${hh}:${mi}`;
};

const formatDay = (x: number): string => new Date(x).toISOString().slice(0, 10);

const formatAxisVal = (v: number): string => {
  if (v === 0) return "0";
  const abs = Math.abs(v);
  if (abs >= 1_000_000) return (v / 1_000_000).toFixed(1) + "M";
  if (abs >= 1_000) return (v / 1_000).toFixed(1) + "K";
  if (abs >= 1) return v.toFixed(1);
  return (v * 100).toFixed(1) + "%";
};

const formatValue = (v: number): string =>
  Math.abs(v) >= 1000
    ? v.toLocaleString(undefined, { maximumFractionDigits: 1 })
    : String(Number(v.toPrecision(4)));

const formatRate = (v: number): string => `${(v * 100).toFixed(1)}%`;

const niceYTicks = (min: number, max: number, count = 5): number[] => {
  if (max === min) return [min];
  const step = (max - min) / (count - 1);
  return Array.from({ length: count }, (_, i) => min + step * i);
};

const plotWidth = (dims: ChartDims) =>
  dims.width - dims.padLeft - dims.padRight;
const plotHeight = (dims: ChartDims) =>
  dims.height - dims.padTop - dims.padBottom;

const boxOf = (dims: ChartDims): PlotBox => ({
  width: dims.width,
  height: dims.height,
  left: dims.padLeft,
  top: dims.padTop,
  plotWidth: plotWidth(dims),
  plotHeight: plotHeight(dims),
});

const makeScaleX =
  ([x0, x1]: XDomain, dims: ChartDims) =>
  (x: number) =>
    x1 === x0
      ? dims.padLeft + plotWidth(dims) / 2
      : dims.padLeft + ((x - x0) / (x1 - x0)) * plotWidth(dims);

const nearestX = (xs: number[], x: number): number | null =>
  xs.reduce<number | null>(
    (best, v) =>
      best === null || Math.abs(v - x) < Math.abs(best - x) ? v : best,
    null,
  );

const YAxis = ({
  dims,
  ticks,
  scaleY,
  format,
  label,
}: {
  dims: ChartDims;
  ticks: number[];
  scaleY: (v: number) => number;
  format: (v: number) => string;
  label: string;
}) => {
  const midY = dims.padTop + plotHeight(dims) / 2;
  return (
    <>
      {ticks.map((v, i) => {
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
              {format(v)}
            </text>
          </g>
        );
      })}
      <text
        x={14}
        y={midY}
        textAnchor="middle"
        fontSize={11}
        fill="#E7664C"
        fontWeight={600}
        transform={`rotate(-90, 14, ${midY})`}
      >
        {label}
      </text>
    </>
  );
};

const XAxisLabels = ({
  dims,
  points,
  scaleX,
}: {
  dims: ChartDims;
  points: { x: number; date: string }[];
  scaleX: (x: number) => number;
}) => {
  const dates = points
    .filter((v, i, arr) => arr.findIndex((a) => a.date === v.date) === i)
    .sort((a, b) => a.x - b.x);
  const maxLabels = Math.floor(plotWidth(dims) / 80);
  const step = Math.max(1, Math.ceil(dates.length / maxLabels));
  return (
    <>
      {dates
        .filter((_, i) => i % step === 0)
        .map((d, i) => (
          <text
            key={`x-${i}`}
            x={scaleX(d.x)}
            y={dims.height - 4}
            textAnchor="middle"
            fontSize={10}
            fill="#69707D"
          >
            {formatDate(d.date)}
          </text>
        ))}
    </>
  );
};

interface ChartPoint {
  x: number;
  value: number;
  date: string;
}

interface LineSeriesData {
  label: string;
  color: string;
  dashArray?: string;
  points: (ChartPoint & { y: number })[];
}

const MultiLineChart = ({
  series,
  dims,
  yLabel,
  yFormatter = formatAxisVal,
  valueFormatter = formatValue,
  interaction,
}: {
  series: LineSeriesData[];
  dims: ChartDims;
  yLabel: string;
  yFormatter?: (v: number) => string;
  valueFormatter?: (v: number) => string;
  interaction: ChartInteraction;
}) => {
  const { domain, hoverX } = interaction;
  const clipId = useClipId();
  const allPoints = useMemo(() => series.flatMap((s) => s.points), [series]);
  const visiblePoints = useMemo(
    () => allPoints.filter((p) => inDomain(p.x, domain)),
    [allPoints, domain],
  );
  const visibleXs = useMemo(
    () => Array.from(new Set(visiblePoints.map((p) => p.x))),
    [visiblePoints],
  );
  const box = boxOf(dims);
  const { svgRef, handlers, brush, isHovered } = usePlotPointer(box, domain, {
    onHover: (x) =>
      interaction.setHoverX(x === null ? null : nearestX(visibleXs, x)),
    onZoom: interaction.zoomTo,
    onReset: interaction.resetZoom,
  });

  if (allPoints.length === 0) return null;

  const yMin = Math.min(...visiblePoints.map((p) => p.value), 0);
  const yMax = Math.max(...visiblePoints.map((p) => p.value), 0.01);
  const plotH = plotHeight(dims);
  const scaleX = makeScaleX(domain, dims);
  const scaleY = (v: number) =>
    dims.padTop + plotH - ((v - yMin) / (yMax - yMin || 1)) * plotH;

  const hoverDate =
    hoverX === null ? null : allPoints.find((p) => p.x === hoverX)?.date;

  return (
    <ZoomScroller
      box={box}
      domain={domain}
      fullDomain={interaction.fullDomain}
      onPan={interaction.panTo}
      testId="time-series-scroll"
    >
      <svg
        ref={svgRef}
        width={dims.width}
        height={dims.height}
        style={{ display: "block", cursor: "crosshair", userSelect: "none" }}
        data-testid="time-series-chart"
        {...handlers}
      >
        <defs>
          <clipPath id={clipId}>
            <rect
              x={dims.padLeft - 5}
              y={0}
              width={plotWidth(dims) + 10}
              height={dims.height - dims.padBottom + 5}
            />
          </clipPath>
        </defs>
        <rect
          x={dims.padLeft}
          y={dims.padTop}
          width={plotWidth(dims)}
          height={plotH}
          fill="transparent"
        />
        <YAxis
          dims={dims}
          ticks={niceYTicks(yMin, yMax)}
          scaleY={scaleY}
          format={yFormatter}
          label={yLabel}
        />
        <XAxisLabels dims={dims} points={visiblePoints} scaleX={scaleX} />

        <g clipPath={`url(#${clipId})`}>
          {series.map((s, si) => {
            if (s.points.length === 0) return null;
            const sorted = [...s.points].sort((a, b) => a.x - b.x);
            const pathD = sorted
              .map(
                (p, i) =>
                  `${i === 0 ? "M" : "L"} ${scaleX(p.x)} ${scaleY(p.value)}`,
              )
              .join(" ");
            const shown = sorted.filter((p) => inDomain(p.x, domain));
            return (
              <g key={si}>
                <path
                  d={pathD}
                  fill="none"
                  stroke={s.color}
                  strokeWidth={2}
                  strokeDasharray={s.dashArray}
                />
                {shown.length <= MAX_MARKERS &&
                  shown.map((p, i) => (
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
                {shown
                  .filter((p) => p.x === hoverX)
                  .map((p, i) => (
                    <circle
                      key={`hover-${i}`}
                      cx={scaleX(p.x)}
                      cy={scaleY(p.value)}
                      r={5}
                      fill={s.color}
                      stroke="#fff"
                      strokeWidth={2}
                    />
                  ))}
              </g>
            );
          })}
        </g>

        <PlotOverlay
          box={box}
          hoverPx={hoverX === null ? null : scaleX(hoverX)}
          brush={brush}
        />
      </svg>
      {isHovered && hoverX !== null && hoverDate && (
        <ChartTooltip
          left={scaleX(hoverX)}
          box={box}
          title={hoverDate}
          rows={series
            .map((s) => ({
              label: s.label,
              color: s.color,
              values: s.points
                .filter((p) => p.x === hoverX)
                .map((p) => valueFormatter(p.value)),
            }))
            .filter((r) => r.values.length > 0)}
        />
      )}
    </ZoomScroller>
  );
};

const AreaChart = ({
  points,
  dims,
  yLabel,
  seriesLabel,
  lineColor,
  fillColor,
  interaction,
}: {
  points: ChartPoint[];
  dims: ChartDims;
  yLabel: string;
  seriesLabel: string;
  lineColor: string;
  fillColor: string;
  interaction: ChartInteraction;
}) => {
  const { domain, hoverX } = interaction;
  const clipId = useClipId();
  const sorted = useMemo(() => [...points].sort((a, b) => a.x - b.x), [points]);
  const visiblePoints = useMemo(
    () => sorted.filter((p) => inDomain(p.x, domain)),
    [sorted, domain],
  );
  const visibleXs = useMemo(
    () => Array.from(new Set(visiblePoints.map((p) => p.x))),
    [visiblePoints],
  );
  const box = boxOf(dims);
  const { svgRef, handlers, brush, isHovered } = usePlotPointer(box, domain, {
    onHover: (x) =>
      interaction.setHoverX(x === null ? null : nearestX(visibleXs, x)),
    onZoom: interaction.zoomTo,
    onReset: interaction.resetZoom,
  });

  if (sorted.length === 0) return null;

  const yMin = 0;
  const yMax = Math.max(...visiblePoints.map((p) => p.value), 0.01);
  const plotH = plotHeight(dims);
  const scaleX = makeScaleX(domain, dims);
  const scaleY = (v: number) =>
    dims.padTop + plotH - ((v - yMin) / (yMax - yMin || 1)) * plotH;
  const baseline = scaleY(0);

  const areaPath =
    `M ${scaleX(sorted[0].x)} ${baseline} ` +
    sorted.map((p) => `L ${scaleX(p.x)} ${scaleY(p.value)}`).join(" ") +
    ` L ${scaleX(sorted[sorted.length - 1].x)} ${baseline} Z`;

  const linePath = sorted
    .map((p, i) => `${i === 0 ? "M" : "L"} ${scaleX(p.x)} ${scaleY(p.value)}`)
    .join(" ");

  const hovered = hoverX === null ? [] : sorted.filter((p) => p.x === hoverX);

  return (
    <ZoomScroller
      box={box}
      domain={domain}
      fullDomain={interaction.fullDomain}
      onPan={interaction.panTo}
      testId="time-series-scroll"
    >
      <svg
        ref={svgRef}
        width={dims.width}
        height={dims.height}
        style={{ display: "block", cursor: "crosshair", userSelect: "none" }}
        data-testid="time-series-chart"
        {...handlers}
      >
        <defs>
          <clipPath id={clipId}>
            <rect
              x={dims.padLeft}
              y={0}
              width={plotWidth(dims)}
              height={dims.height - dims.padBottom}
            />
          </clipPath>
        </defs>
        <rect
          x={dims.padLeft}
          y={dims.padTop}
          width={plotWidth(dims)}
          height={plotH}
          fill="transparent"
        />
        <YAxis
          dims={dims}
          ticks={niceYTicks(yMin, yMax)}
          scaleY={scaleY}
          format={(v) => `${(v * 100).toFixed(0)}%`}
          label={yLabel}
        />
        <XAxisLabels dims={dims} points={visiblePoints} scaleX={scaleX} />

        <g clipPath={`url(#${clipId})`}>
          <path d={areaPath} fill={fillColor} />
          <path d={linePath} fill="none" stroke={lineColor} strokeWidth={2} />
          {hovered.map((p, i) => (
            <circle
              key={i}
              cx={scaleX(p.x)}
              cy={scaleY(p.value)}
              r={4}
              fill={lineColor}
              stroke="#fff"
              strokeWidth={2}
            />
          ))}
        </g>

        <PlotOverlay
          box={box}
          hoverPx={hoverX === null ? null : scaleX(hoverX)}
          brush={brush}
        />
      </svg>
      {isHovered && hoverX !== null && hovered.length > 0 && (
        <ChartTooltip
          left={scaleX(hoverX)}
          box={box}
          title={hovered[0].date}
          rows={[
            {
              label: seriesLabel,
              color: lineColor,
              values: hovered.map((p) => formatRate(p.value)),
            },
          ]}
        />
      )}
    </ZoomScroller>
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
  // Name of the window length being plotted, e.g. "Daily".
  granularityLabel?: string;
}

const toX = (m: FeatureMetric) => new Date(m.metric_date).getTime();

const TimeSeriesAnalysis = ({
  metrics,
  featureType,
  granularityLabel,
}: TimeSeriesAnalysisProps) => {
  const [range, setRange] = useState("all");
  const [hoverX, setHoverX] = useState<number | null>(null);

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

  const fullDomain = useMemo<XDomain | null>(() => {
    if (sorted.length === 0) return null;
    const xs = sorted.map(toX);
    return [Math.min(...xs), Math.max(...xs)];
  }, [sorted]);

  const zoom = useZoom(fullDomain, DAY_MS);
  const { domain, reset } = zoom;

  // A new time range or new data starts zoomed out.
  useEffect(() => {
    reset();
  }, [range, metrics, reset]);

  const interaction: ChartInteraction | null =
    domain && fullDomain
      ? {
          domain,
          fullDomain,
          // After a zoom the last hovered point can sit outside the window.
          hoverX: hoverX !== null && inDomain(hoverX, domain) ? hoverX : null,
          setHoverX,
          zoomTo: zoom.zoomTo,
          panTo: zoom.panTo,
          resetZoom: reset,
        }
      : null;

  const isNumeric = featureType === "numeric";
  const windows = granularityLabel
    ? `${granularityLabel.toLowerCase()} windows `
    : "";

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
        <EuiFlexItem grow={false}>
          <EuiFlexGroup gutterSize="xs" alignItems="center" responsive={false}>
            <EuiFlexItem grow={false}>
              <EuiButtonIcon
                iconType="magnifyWithPlus"
                aria-label="Zoom in"
                title="Zoom in"
                isDisabled={!zoom.canZoomIn}
                onClick={zoom.zoomIn}
              />
            </EuiFlexItem>
            <EuiFlexItem grow={false}>
              <EuiButtonIcon
                iconType="magnifyWithMinus"
                aria-label="Zoom out"
                title="Zoom out"
                isDisabled={!zoom.isZoomed}
                onClick={zoom.zoomOut}
              />
            </EuiFlexItem>
            <EuiFlexItem grow={false}>
              <EuiButtonEmpty
                size="xs"
                isDisabled={!zoom.isZoomed}
                onClick={reset}
              >
                Reset zoom
              </EuiButtonEmpty>
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
        </EuiFlexItem>
      </EuiFlexGroup>

      {domain && (
        <p style={{ color: "#69707D", fontSize: 12, marginTop: 8 }}>
          {`Showing ${windows}${formatDay(domain[0])} – ${formatDay(domain[1])}. Drag across a chart to zoom in, scroll sideways to move, double-click to reset.`}
        </p>
      )}

      <EuiSpacer size="m" />

      {!interaction ? (
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
        <NumericTimeSeries
          metrics={sorted}
          toX={toX}
          interaction={interaction}
        />
      ) : (
        <CategoricalTimeSeries
          metrics={sorted}
          toX={toX}
          interaction={interaction}
        />
      )}
    </EuiPanel>
  );
};

const NumericTimeSeries = ({
  metrics,
  toX,
  interaction,
}: {
  metrics: FeatureMetric[];
  toX: (m: FeatureMetric) => number;
  interaction: ChartInteraction;
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
        <MultiLineChart
          series={driftSeries}
          dims={DIMS}
          yLabel="Metric Value"
          interaction={interaction}
        />
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
        <AreaChart
          points={nullPoints}
          dims={{ ...DIMS, height: 160 }}
          yLabel="Percentage (%)"
          seriesLabel="Null rate"
          lineColor="#BD271E"
          fillColor="rgba(189, 39, 30, 0.15)"
          interaction={interaction}
        />
      </div>
    </>
  );
};

const CategoricalTimeSeries = ({
  metrics,
  toX,
  interaction,
}: {
  metrics: FeatureMetric[];
  toX: (m: FeatureMetric) => number;
  interaction: ChartInteraction;
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
        <MultiLineChart
          series={cardinalitySeries}
          dims={DIMS}
          yLabel="Count"
          interaction={interaction}
        />
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
        <MultiLineChart
          series={shareSeries}
          dims={DIMS}
          yLabel="Percentage (%)"
          yFormatter={shareYFormatter}
          valueFormatter={(v) => `${v.toFixed(1)}%`}
          interaction={interaction}
        />
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
        <AreaChart
          points={nullPoints}
          dims={{ ...DIMS, height: 160 }}
          yLabel="Percentage (%)"
          seriesLabel="Null rate"
          lineColor="#BD271E"
          fillColor="rgba(189, 39, 30, 0.15)"
          interaction={interaction}
        />
      </div>
    </>
  );
};

export default TimeSeriesAnalysis;
