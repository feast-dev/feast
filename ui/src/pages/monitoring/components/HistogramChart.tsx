import React, { useEffect, useMemo, useState } from "react";
import {
  EuiPanel,
  EuiTitle,
  EuiSpacer,
  EuiText,
  EuiModal,
  EuiModalHeader,
  EuiModalHeaderTitle,
  EuiModalBody,
  EuiButtonIcon,
  EuiFlexGroup,
  EuiFlexItem,
  EuiToolTip,
  EuiButtonEmpty,
} from "@elastic/eui";
import type {
  NumericHistogram,
  CategoricalHistogram,
} from "../../../queries/useMonitoringApi";
import {
  ChartTooltip,
  PlotOverlay,
  ZoomScroller,
  useClipId,
  usePlotPointer,
  useZoom,
} from "./chartZoom";
import type { PlotBox, XDomain } from "./chartZoom";

const BAR_COLOR = "#006BB4";
const BAR_COLOR_BASELINE = "#BD271E55";

interface ChartDimensions {
  chartHeight: number;
  axisHeight: number;
  leftPad: number;
  barGap: number;
  minBarWidth: number;
  targetBarsWidth: number;
  fontSize: number;
  xTickCount: number;
}

const COMPACT: ChartDimensions = {
  chartHeight: 160,
  axisHeight: 28,
  leftPad: 54,
  barGap: 2,
  minBarWidth: 6,
  targetBarsWidth: 460,
  fontSize: 10,
  xTickCount: 2,
};

const EXPANDED: ChartDimensions = {
  chartHeight: 400,
  axisHeight: 48,
  leftPad: 72,
  barGap: 3,
  minBarWidth: 12,
  targetBarsWidth: 800,
  fontSize: 12,
  xTickCount: 6,
};

const formatNumber = (val: number, compact: boolean): string => {
  if (val === 0) return "0";
  const abs = Math.abs(val);
  if (compact && abs >= 1_000_000) return (val / 1_000_000).toFixed(1) + "M";
  if (compact && abs >= 1_000) return (val / 1_000).toFixed(1) + "K";
  if (abs >= 1)
    return val.toLocaleString(undefined, { maximumFractionDigits: 1 });
  if (abs >= 0.01) return val.toFixed(2);
  return val.toExponential(1);
};

const BAR_COLOR_HOVER = "#00508A";

// One numeric histogram. Hover a bar to read its count; drag to zoom into a
// range of bars, scroll sideways to move while zoomed, double-click to reset.
const NumericHistogramSvg = ({
  histogram,
  baseline,
  dim,
}: {
  histogram: NumericHistogram;
  baseline: NumericHistogram | null | undefined;
  dim: ChartDimensions;
}) => {
  const numBars = histogram.counts.length;
  const isCompact = dim === COMPACT;
  const barWidth = Math.max(
    Math.floor(dim.targetBarsWidth / Math.max(numBars, 1)) - dim.barGap,
    dim.minBarWidth,
  );
  const barsWidth = (barWidth + dim.barGap) * numBars;
  // Room above the plot so the top axis label is not cut off.
  const top = dim.fontSize;
  const box: PlotBox = {
    width: Math.max(dim.leftPad + barsWidth + 24, 280),
    height: top + dim.chartHeight + dim.axisHeight,
    left: dim.leftPad,
    top,
    plotWidth: barsWidth,
    plotHeight: dim.chartHeight,
  };
  const bottom = top + dim.chartHeight;

  // The x axis is the bar index: bar i covers [i, i + 1).
  const fullDomain = useMemo<XDomain>(() => [0, numBars], [numBars]);
  const zoom = useZoom(numBars > 0 ? fullDomain : null, Math.min(3, numBars));
  const { reset } = zoom;
  const domain = zoom.domain ?? fullDomain;
  const [hoverBin, setHoverBin] = useState<number | null>(null);
  const clipId = useClipId();

  // Another histogram (for example a different View) starts zoomed out.
  useEffect(() => {
    reset();
  }, [histogram, reset]);

  const { svgRef, handlers, brush, isHovered } = usePlotPointer(box, domain, {
    onHover: (x) =>
      setHoverBin(x === null ? null : Math.min(Math.floor(x), numBars - 1)),
    // Snap the selection to whole bars.
    onZoom: ([a, b]) => zoom.zoomTo([Math.floor(a), Math.ceil(b)]),
    onReset: reset,
  });

  if (numBars === 0) return null;

  const [d0, d1] = domain;
  const pxPerBin = box.plotWidth / (d1 - d0);
  const binX = (i: number) => box.left + (i - d0) * pxPerBin;
  const gap = Math.min(dim.barGap, pxPerBin / 4);
  const first = Math.max(0, Math.floor(d0));
  const last = Math.min(numBars - 1, Math.ceil(d1) - 1);
  const visible = Array.from({ length: last - first + 1 }, (_, k) => first + k);

  // Value at the left edge of bar i (i = numBars is the right end).
  const edgeValue = (i: number) =>
    i < histogram.bins.length
      ? histogram.bins[i]
      : histogram.bins[histogram.bins.length - 1] +
        (i - histogram.bins.length + 1) * histogram.bin_width;

  // Fit the height to the bars in view, so small bars grow when zoomed in.
  const maxCount = Math.max(
    ...visible.map((i) => histogram.counts[i]),
    ...(baseline ? visible.map((i) => baseline.counts[i] ?? 0) : []),
    1,
  );
  const barTop = (count: number) =>
    bottom - (count / maxCount) * dim.chartHeight;

  const yTicks = [0, 0.25, 0.5, 0.75, 1].map((f) => ({
    label: formatNumber(Math.round(maxCount * f), isCompact),
    y: bottom - f * dim.chartHeight,
  }));

  // Axis labels sit on bar edges that are fully in view.
  const edges = Array.from(
    { length: Math.floor(d1) - Math.ceil(d0) + 1 },
    (_, k) => Math.ceil(d0) + k,
  );
  const xStep = Math.max(1, Math.floor((edges.length - 1) / dim.xTickCount));
  const xTicks = edges.filter((_, k) => k % xStep === 0);
  const lastEdge = edges[edges.length - 1];
  if (xTicks.length > 0 && xTicks[xTicks.length - 1] !== lastEdge) {
    if (lastEdge - xTicks[xTicks.length - 1] < xStep / 2) xTicks.pop();
    xTicks.push(lastEdge);
  }

  const hovered =
    isHovered && hoverBin !== null && hoverBin >= first && hoverBin <= last
      ? hoverBin
      : null;

  return (
    <>
      <EuiFlexGroup gutterSize="xs" alignItems="center" responsive={false}>
        <EuiFlexItem grow={false}>
          <EuiButtonIcon
            iconType="magnifyWithPlus"
            aria-label="Zoom in"
            title="Zoom in"
            size="xs"
            isDisabled={!zoom.canZoomIn}
            onClick={zoom.zoomIn}
          />
        </EuiFlexItem>
        <EuiFlexItem grow={false}>
          <EuiButtonIcon
            iconType="magnifyWithMinus"
            aria-label="Zoom out"
            title="Zoom out"
            size="xs"
            isDisabled={!zoom.isZoomed}
            onClick={zoom.zoomOut}
          />
        </EuiFlexItem>
        <EuiFlexItem grow={false}>
          <EuiButtonEmpty size="xs" isDisabled={!zoom.isZoomed} onClick={reset}>
            Reset zoom
          </EuiButtonEmpty>
        </EuiFlexItem>
        <EuiFlexItem grow={false}>
          <EuiText size="xs" color="subdued">
            Drag to zoom, scroll sideways to move.
          </EuiText>
        </EuiFlexItem>
      </EuiFlexGroup>
      <ZoomScroller
        box={box}
        domain={domain}
        fullDomain={fullDomain}
        onPan={zoom.panTo}
        testId="histogram-scroll"
      >
        <svg
          ref={svgRef}
          width={box.width}
          height={box.height}
          role="img"
          aria-label="Histogram"
          style={{ display: "block", cursor: "crosshair", userSelect: "none" }}
          {...handlers}
        >
          <defs>
            <clipPath id={clipId}>
              <rect
                x={box.left}
                y={0}
                width={box.plotWidth}
                height={bottom + 1}
              />
            </clipPath>
          </defs>
          <rect
            x={box.left}
            y={top}
            width={box.plotWidth}
            height={dim.chartHeight}
            fill="transparent"
          />
          {yTicks.map((t, i) => (
            <g key={i}>
              <line
                x1={box.left}
                y1={t.y}
                x2={box.left + box.plotWidth}
                y2={t.y}
                stroke="#EDF0F5"
                strokeWidth={1}
              />
              <text
                x={box.left - 6}
                y={t.y + 4}
                fontSize={dim.fontSize}
                fill="#98A2B3"
                textAnchor="end"
              >
                {t.label}
              </text>
            </g>
          ))}
          <g clipPath={`url(#${clipId})`}>
            {visible.map((i) => {
              const x = binX(i) + gap / 2;
              const w = Math.max(pxPerBin - gap, 1);
              const count = histogram.counts[i];
              const base = baseline?.counts[i] ?? 0;
              return (
                <g key={i}>
                  {base > 0 && (
                    <rect
                      x={x}
                      y={barTop(base)}
                      width={w}
                      height={bottom - barTop(base)}
                      fill={BAR_COLOR_BASELINE}
                      rx={1}
                    />
                  )}
                  <rect
                    x={x}
                    y={barTop(count)}
                    width={w}
                    height={Math.max(bottom - barTop(count), 1)}
                    fill={hovered === i ? BAR_COLOR_HOVER : BAR_COLOR}
                    rx={1}
                    opacity={0.85}
                  />
                </g>
              );
            })}
          </g>
          <line
            x1={box.left}
            y1={bottom}
            x2={box.left + box.plotWidth}
            y2={bottom}
            stroke="#D3DAE6"
            strokeWidth={1}
          />
          {xTicks.map((edge) => (
            <text
              key={edge}
              x={binX(edge)}
              y={bottom + dim.fontSize + 6}
              fontSize={dim.fontSize}
              fill="#69707D"
              textAnchor="middle"
            >
              {formatNumber(edgeValue(edge), isCompact)}
            </text>
          ))}
          <PlotOverlay box={box} hoverPx={null} brush={brush} />
        </svg>
        {hovered !== null && (
          <ChartTooltip
            left={binX(hovered + 0.5)}
            box={box}
            title={`${formatNumber(edgeValue(hovered), false)} – ${formatNumber(edgeValue(hovered + 1), false)}`}
            rows={[
              {
                label: "Count",
                color: BAR_COLOR,
                values: [histogram.counts[hovered].toLocaleString()],
              },
              ...(baseline
                ? [
                    {
                      label: "Baseline",
                      color: BAR_COLOR_BASELINE,
                      values: [
                        (baseline.counts[hovered] ?? 0).toLocaleString(),
                      ],
                    },
                  ]
                : []),
            ]}
          />
        )}
      </ZoomScroller>
    </>
  );
};

const NumericHistogramChart = ({
  histogram,
  baseline,
  title,
}: {
  histogram: NumericHistogram;
  baseline?: NumericHistogram | null;
  title?: string;
}) => {
  const [expanded, setExpanded] = useState(false);

  return (
    <>
      <EuiPanel hasBorder>
        <EuiFlexGroup
          justifyContent="spaceBetween"
          alignItems="center"
          gutterSize="none"
        >
          <EuiFlexItem grow={false}>
            {title && (
              <EuiTitle size="xxs">
                <h4>{title}</h4>
              </EuiTitle>
            )}
          </EuiFlexItem>
          <EuiFlexItem grow={false}>
            <EuiToolTip content="Expand histogram">
              <EuiButtonIcon
                iconType="expand"
                aria-label="Expand histogram"
                size="s"
                color="text"
                onClick={() => setExpanded(true)}
              />
            </EuiToolTip>
          </EuiFlexItem>
        </EuiFlexGroup>
        {title && <EuiSpacer size="s" />}
        <div style={{ overflowX: "auto" }}>
          <NumericHistogramSvg
            histogram={histogram}
            baseline={baseline}
            dim={COMPACT}
          />
        </div>
        {baseline && (
          <EuiText size="xs" color="subdued">
            <span
              style={{
                display: "inline-block",
                width: 10,
                height: 10,
                backgroundColor: BAR_COLOR_BASELINE,
                marginRight: 4,
              }}
            />
            Baseline
          </EuiText>
        )}
      </EuiPanel>

      {expanded && (
        <EuiModal onClose={() => setExpanded(false)} maxWidth={960}>
          <EuiModalHeader>
            <EuiModalHeaderTitle>{title || "Histogram"}</EuiModalHeaderTitle>
          </EuiModalHeader>
          <EuiModalBody>
            <div style={{ overflowX: "auto" }}>
              <NumericHistogramSvg
                histogram={histogram}
                baseline={baseline}
                dim={EXPANDED}
              />
            </div>
            {baseline && (
              <>
                <EuiSpacer size="s" />
                <EuiText size="xs" color="subdued">
                  <span
                    style={{
                      display: "inline-block",
                      width: 10,
                      height: 10,
                      backgroundColor: BAR_COLOR_BASELINE,
                      marginRight: 4,
                    }}
                  />
                  Baseline
                </EuiText>
              </>
            )}
          </EuiModalBody>
        </EuiModal>
      )}
    </>
  );
};

const LABEL_WIDTH = 60;
const BAR_MAX_WIDTH = 320;
const COUNT_PAD = 80;

const LABEL_WIDTH_EXP = 120;
const BAR_MAX_WIDTH_EXP = 560;
const COUNT_PAD_EXP = 100;

const renderCategoricalSvg = (
  histogram: CategoricalHistogram,
  isExpanded: boolean,
) => {
  const labelW = isExpanded ? LABEL_WIDTH_EXP : LABEL_WIDTH;
  const barMax = isExpanded ? BAR_MAX_WIDTH_EXP : BAR_MAX_WIDTH;
  const countPad = isExpanded ? COUNT_PAD_EXP : COUNT_PAD;
  const totalW = labelW + barMax + countPad;
  const truncLen = isExpanded ? 20 : 8;
  const fontSize = isExpanded ? 13 : 12;
  const barHeight = isExpanded ? 30 : 24;
  const rowHeight = barHeight + 6;

  const maxCount = Math.max(...histogram.values.map((v) => v.count), 1);
  const chartHeight = histogram.values.length * rowHeight;

  return (
    <svg
      width={totalW}
      height={chartHeight}
      role="img"
      aria-label="Category chart"
    >
      {histogram.values.map((v, i) => {
        const width = (v.count / maxCount) * barMax;
        const y = i * rowHeight;
        return (
          <g key={v.value}>
            <text
              x={labelW - 8}
              y={y + barHeight / 2 + 4}
              fontSize={fontSize}
              fill="#343741"
              textAnchor="end"
            >
              {v.value.length > truncLen
                ? v.value.slice(0, truncLen) + "…"
                : v.value}
            </text>
            <rect
              x={labelW}
              y={y + 2}
              width={Math.max(width, 2)}
              height={barHeight - 4}
              fill={BAR_COLOR}
              rx={2}
              opacity={0.85}
            >
              <title>{`${v.value}: ${v.count.toLocaleString()}`}</title>
            </rect>
            <text
              x={labelW + width + 6}
              y={y + barHeight / 2 + 4}
              fontSize={fontSize - 1}
              fill="#69707D"
            >
              {v.count.toLocaleString()}
            </text>
          </g>
        );
      })}
    </svg>
  );
};

const CategoricalHistogramChart = ({
  histogram,
  title,
}: {
  histogram: CategoricalHistogram;
  title?: string;
}) => {
  const [expanded, setExpanded] = useState(false);

  return (
    <>
      <EuiPanel hasBorder>
        <EuiFlexGroup
          justifyContent="spaceBetween"
          alignItems="center"
          gutterSize="none"
        >
          <EuiFlexItem grow={false}>
            {title && (
              <EuiTitle size="xxs">
                <h4>{title}</h4>
              </EuiTitle>
            )}
          </EuiFlexItem>
          <EuiFlexItem grow={false}>
            <EuiToolTip content="Expand chart">
              <EuiButtonIcon
                iconType="expand"
                aria-label="Expand chart"
                size="s"
                color="text"
                onClick={() => setExpanded(true)}
              />
            </EuiToolTip>
          </EuiFlexItem>
        </EuiFlexGroup>
        {title && <EuiSpacer size="s" />}
        <div style={{ overflowX: "auto" }}>
          {renderCategoricalSvg(histogram, false)}
        </div>
        <EuiText size="xs" color="subdued">
          {histogram.unique_count} unique values
          {histogram.other_count > 0 &&
            ` (${histogram.other_count.toLocaleString()} in other categories)`}
        </EuiText>
      </EuiPanel>

      {expanded && (
        <EuiModal onClose={() => setExpanded(false)} maxWidth={960}>
          <EuiModalHeader>
            <EuiModalHeaderTitle>
              {title || "Category Distribution"}
            </EuiModalHeaderTitle>
          </EuiModalHeader>
          <EuiModalBody>
            <div style={{ overflowX: "auto" }}>
              {renderCategoricalSvg(histogram, true)}
            </div>
            <EuiSpacer size="s" />
            <EuiText size="xs" color="subdued">
              {histogram.unique_count} unique values
              {histogram.other_count > 0 &&
                ` (${histogram.other_count.toLocaleString()} in other categories)`}
            </EuiText>
          </EuiModalBody>
        </EuiModal>
      )}
    </>
  );
};

export { NumericHistogramChart, CategoricalHistogramChart };
