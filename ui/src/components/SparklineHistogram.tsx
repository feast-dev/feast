import React from "react";
import { HistogramDataType } from "../parsers/featureViewSummaryStatistics";
import { extent } from "d3-array";
import { scaleLinear } from "d3";
import { EuiBadge, useEuiTheme } from "@elastic/eui";

interface SparklineHistogramProps {
  data: HistogramDataType;
}

const SparklineHistogram = ({ data }: SparklineHistogramProps) => {
  const width = 100;
  const height = 24;

  const yMax = height - 2;

  const { euiTheme } = useEuiTheme();

  if (data.length > 0) {
    const x0Extent = extent(data, (d) => d.x0) as [number, number];
    const xScale = scaleLinear()
      .domain(x0Extent)
      .range([0, width - width / data.length]);

    const yExtent = extent(data, (d) => d.count) as [number, number];
    const yScale = scaleLinear().domain(yExtent).range([0, yMax]);

    return (
      <svg width={width} height={height}>
        <rect
          x={0}
          y={height - 1}
          width={width}
          height={1}
          fill={euiTheme.colors.mediumShade}
        />
        {data.map((d) => {
          const barHeight = yScale(d.count);

          return (
            <rect
              key={d.x0}
              width={width / data.length}
              height={barHeight}
              y={yMax - barHeight}
              x={xScale(d.x0)}
              fill={euiTheme.colors.primary}
            />
          );
        })}
      </svg>
    );
  } else {
    return <EuiBadge color="warning">histogram n/a</EuiBadge>;
  }
};

export default SparklineHistogram;
