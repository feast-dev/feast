import React from "react";
import {
  EuiPanel,
  EuiTitle,
  EuiSpacer,
  EuiText,
} from "@elastic/eui";
import type {
  NumericHistogram,
  CategoricalHistogram,
} from "../../../queries/useMonitoringApi";

const BAR_COLOR = "#006BB4";
const BAR_COLOR_BASELINE = "#BD271E55";
const CHART_HEIGHT = 160;
const AXIS_HEIGHT = 24;
const LEFT_PAD = 50;

const NumericHistogramChart = ({
  histogram,
  baseline,
  title,
}: {
  histogram: NumericHistogram;
  baseline?: NumericHistogram | null;
  title?: string;
}) => {
  const maxCount = Math.max(...histogram.counts, 1);
  const numBars = histogram.counts.length;
  const barWidth = Math.max(Math.floor(460 / numBars) - 2, 6);
  const barsWidth = (barWidth + 2) * numBars;
  const svgWidth = LEFT_PAD + barsWidth + 20;

  const yTicks = [0, 0.25, 0.5, 0.75, 1].map((f) => ({
    value: Math.round(maxCount * f),
    y: CHART_HEIGHT - f * CHART_HEIGHT,
  }));

  return (
    <EuiPanel hasBorder>
      {title && (
        <>
          <EuiTitle size="xxs">
            <h4>{title}</h4>
          </EuiTitle>
          <EuiSpacer size="s" />
        </>
      )}
      <div style={{ overflowX: "auto" }}>
        <svg
          width={Math.max(svgWidth, 280)}
          height={CHART_HEIGHT + AXIS_HEIGHT}
          role="img"
          aria-label="Histogram"
        >
          {yTicks.map((t) => (
            <g key={t.value}>
              <line
                x1={LEFT_PAD}
                y1={t.y}
                x2={LEFT_PAD + barsWidth}
                y2={t.y}
                stroke="#EDF0F5"
                strokeWidth={1}
              />
              <text
                x={LEFT_PAD - 6}
                y={t.y + 4}
                fontSize={10}
                fill="#98A2B3"
                textAnchor="end"
              >
                {t.value.toLocaleString()}
              </text>
            </g>
          ))}
          {histogram.counts.map((count, i) => {
            const height = (count / maxCount) * CHART_HEIGHT;
            const x = LEFT_PAD + i * (barWidth + 2);
            const binStart = histogram.bins[i];
            const binEnd =
              i < histogram.bins.length - 1
                ? histogram.bins[i + 1]
                : binStart + histogram.bin_width;
            const baselineHeight =
              baseline && baseline.counts[i]
                ? (baseline.counts[i] / maxCount) * CHART_HEIGHT
                : 0;

            return (
              <g key={i}>
                {baselineHeight > 0 && (
                  <rect
                    x={x}
                    y={CHART_HEIGHT - baselineHeight}
                    width={barWidth}
                    height={baselineHeight}
                    fill={BAR_COLOR_BASELINE}
                    rx={1}
                  />
                )}
                <rect
                  x={x}
                  y={CHART_HEIGHT - height}
                  width={barWidth}
                  height={Math.max(height, 1)}
                  fill={BAR_COLOR}
                  rx={1}
                  opacity={0.85}
                >
                  <title>{`${binStart.toFixed(2)} – ${binEnd.toFixed(2)}: ${count.toLocaleString()}`}</title>
                </rect>
              </g>
            );
          })}
          <line
            x1={LEFT_PAD}
            y1={CHART_HEIGHT}
            x2={LEFT_PAD + barsWidth}
            y2={CHART_HEIGHT}
            stroke="#D3DAE6"
            strokeWidth={1}
          />
          <text
            x={LEFT_PAD}
            y={CHART_HEIGHT + 16}
            fontSize={10}
            fill="#69707D"
          >
            {histogram.bins[0]?.toLocaleString(undefined, { maximumFractionDigits: 1 })}
          </text>
          <text
            x={LEFT_PAD + barsWidth}
            y={CHART_HEIGHT + 16}
            fontSize={10}
            fill="#69707D"
            textAnchor="end"
          >
            {histogram.bins[histogram.bins.length - 1]?.toLocaleString(undefined, { maximumFractionDigits: 1 })}
          </text>
        </svg>
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
  );
};

const LABEL_WIDTH = 60;
const BAR_MAX_WIDTH = 320;
const COUNT_PAD = 80;
const CAT_SVG_WIDTH = LABEL_WIDTH + BAR_MAX_WIDTH + COUNT_PAD;

const CategoricalHistogramChart = ({
  histogram,
  title,
}: {
  histogram: CategoricalHistogram;
  title?: string;
}) => {
  const maxCount = Math.max(
    ...histogram.values.map((v) => v.count),
    1,
  );
  const barHeight = 24;
  const rowHeight = barHeight + 6;
  const chartHeight = histogram.values.length * rowHeight;

  return (
    <EuiPanel hasBorder>
      {title && (
        <>
          <EuiTitle size="xxs">
            <h4>{title}</h4>
          </EuiTitle>
          <EuiSpacer size="s" />
        </>
      )}
      <div style={{ overflowX: "auto" }}>
        <svg
          width={CAT_SVG_WIDTH}
          height={chartHeight}
          role="img"
          aria-label="Category chart"
        >
          {histogram.values.map((v, i) => {
            const width = (v.count / maxCount) * BAR_MAX_WIDTH;
            const y = i * rowHeight;
            return (
              <g key={v.value}>
                <text
                  x={LABEL_WIDTH - 8}
                  y={y + 16}
                  fontSize={12}
                  fill="#343741"
                  textAnchor="end"
                >
                  {v.value.length > 8 ? v.value.slice(0, 8) + "…" : v.value}
                </text>
                <rect
                  x={LABEL_WIDTH}
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
                  x={LABEL_WIDTH + width + 6}
                  y={y + 16}
                  fontSize={11}
                  fill="#69707D"
                >
                  {v.count.toLocaleString()}
                </text>
              </g>
            );
          })}
        </svg>
      </div>
      <EuiText size="xs" color="subdued">
        {histogram.unique_count} unique values
        {histogram.other_count > 0 &&
          ` (${histogram.other_count.toLocaleString()} in other categories)`}
      </EuiText>
    </EuiPanel>
  );
};

export { NumericHistogramChart, CategoricalHistogramChart };
