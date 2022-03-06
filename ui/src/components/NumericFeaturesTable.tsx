import { EuiBasicTable } from "@elastic/eui";
import React from "react";
import { NumericColumnSummaryStatisticType } from "../parsers/featureViewSummaryStatistics";
import SparklineHistogram from "./SparklineHistogram";

interface NumericFeaturesTableProps {
  data: NumericColumnSummaryStatisticType[];
}

const NumericFeaturesTable = ({ data }: NumericFeaturesTableProps) => {
  const columns = [
    { name: "Name", field: "name" },
    {
      name: "Value Type",
      field: "valueType",
    },
    {
      name: "Sample",
      render: (statistics: NumericColumnSummaryStatisticType) => {
        return (
          <React.Fragment>
            {statistics && statistics.sampleValues.join(",")}
          </React.Fragment>
        );
      },
    },
    {
      name: "Min/Max",
      render: (statistics: NumericColumnSummaryStatisticType) => {
        return statistics.min !== undefined && statistics.max !== undefined
          ? `${statistics.min}/${statistics.max}`
          : undefined;
      },
    },
    { name: "zeros", field: "proportionOfZeros" },
    { name: "missing", field: "proportionMissing" },
    {
      name: "Sparklines",
      render: (statistics: NumericColumnSummaryStatisticType) => {
        if (statistics && statistics.histogram) {
          return <SparklineHistogram data={statistics.histogram} />;
        } else {
          return "";
        }
      },
    },
  ];

  const getRowProps = (item: NumericColumnSummaryStatisticType) => {
    return {
      "data-test-subj": `row-${item.name}`,
    };
  };

  return (
    <EuiBasicTable columns={columns} items={data} rowProps={getRowProps} />
  );
};

export default NumericFeaturesTable;
