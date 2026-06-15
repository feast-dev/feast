import React, { useState } from "react";
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
} from "@elastic/eui";
import type {
  NumericHistogram,
  CategoricalHistogram,
} from "../../../queries/useMonitoringApi";

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

const renderNumericSvg = (
  histogram: NumericHistogram,
  baseline: NumericHistogram | null | undefined,
  dim: ChartDimensions,
) => {
  const maxCount = Math.max(
    ...histogram.counts,
    ...(baseline ? baseline.counts : []),
    1,
  );
  const numBars = histogram.counts.length;
  const barWidth = Math.max(
    Math.floor(dim.targetBarsWidth / numBars) - dim.barGap,
    dim.minBarWidth,
  );
  const barsWidth = (barWidth + dim.barGap) * numBars;
  const svgWidth = dim.leftPad + barsWidth + 24;
  const isCompact = dim === COMPACT;

  const yTickFractions = [0, 0.25, 0.5, 0.75, 1];
  const yTicks = yTickFractions.map((f) => ({
    label: formatNumber(Math.round(maxCount * f), isCompact),
    y: dim.chartHeight - f * dim.chartHeight,
  }));

  const xTickStep = Math.max(1, Math.floor(numBars / dim.xTickCount));
  const xTicks: { label: string; x: number }[] = [];
  for (let i = 0; i < numBars; i += xTickStep) {
    xTicks.push({
      label: formatNumber(histogram.bins[i], isCompact),
      x: dim.leftPad + i * (barWidth + dim.barGap) + barWidth / 2,
    });
  }
  if (numBars > 0) {
    const lastBin = histogram.bins[histogram.bins.length - 1];
    xTicks.push({
      label: formatNumber(lastBin, isCompact),
      x: dim.leftPad + (numBars - 1) * (barWidth + dim.barGap) + barWidth / 2,
    });
  }

  return (
    <svg
      width={Math.max(svgWidth, 280)}
      height={dim.chartHeight + dim.axisHeight}
      role="img"
      aria-label="Histogram"
    >
      {yTicks.map((t, i) => (
        <g key={i}>
          <line
            x1={dim.leftPad}
            y1={t.y}
            x2={dim.leftPad + barsWidth}
            y2={t.y}
            stroke="#EDF0F5"
            strokeWidth={1}
          />
          <text
            x={dim.leftPad - 6}
            y={t.y + 4}
            fontSize={dim.fontSize}
            fill="#98A2B3"
            textAnchor="end"
          >
            {t.label}
          </text>
        </g>
      ))}
      {histogram.counts.map((count, i) => {
        const height = (count / maxCount) * dim.chartHeight;
        const x = dim.leftPad + i * (barWidth + dim.barGap);
        const binStart = histogram.bins[i];
        const binEnd =
          i < histogram.bins.length - 1
            ? histogram.bins[i + 1]
            : binStart + histogram.bin_width;
        const baselineHeight =
          baseline && baseline.counts[i]
            ? (baseline.counts[i] / maxCount) * dim.chartHeight
            : 0;

        return (
          <g key={i}>
            {baselineHeight > 0 && (
              <rect
                x={x}
                y={dim.chartHeight - baselineHeight}
                width={barWidth}
                height={baselineHeight}
                fill={BAR_COLOR_BASELINE}
                rx={1}
              />
            )}
            <rect
              x={x}
              y={dim.chartHeight - height}
              width={barWidth}
              height={Math.max(height, 1)}
              fill={BAR_COLOR}
              rx={1}
              opacity={0.85}
            >
              <title>{`${formatNumber(binStart, false)} – ${formatNumber(binEnd, false)}: ${count.toLocaleString()}`}</title>
            </rect>
          </g>
        );
      })}
      <line
        x1={dim.leftPad}
        y1={dim.chartHeight}
        x2={dim.leftPad + barsWidth}
        y2={dim.chartHeight}
        stroke="#D3DAE6"
        strokeWidth={1}
      />
      {xTicks.map((t, i) => (
        <text
          key={i}
          x={t.x}
          y={dim.chartHeight + dim.fontSize + 6}
          fontSize={dim.fontSize}
          fill="#69707D"
          textAnchor="middle"
        >
          {t.label}
        </text>
      ))}
    </svg>
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
          {renderNumericSvg(histogram, baseline, COMPACT)}
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
              {renderNumericSvg(histogram, baseline, EXPANDED)}
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
const CAT_SVG_WIDTH = LABEL_WIDTH + BAR_MAX_WIDTH + COUNT_PAD;

const LABEL_WIDTH_EXP = 120;
const BAR_MAX_WIDTH_EXP = 560;
const COUNT_PAD_EXP = 100;
const CAT_SVG_WIDTH_EXP = LABEL_WIDTH_EXP + BAR_MAX_WIDTH_EXP + COUNT_PAD_EXP;

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
