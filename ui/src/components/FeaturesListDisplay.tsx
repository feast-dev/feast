import React from "react";
import { enabledFeatureStatistics } from "../flags";
import { EuiBasicTable, EuiLoadingSpinner, EuiBadge } from "@elastic/eui";
import { FeastFeatureColumnType } from "../parsers/feastFeatureViews";
import useLoadFeatureViewSummaryStatistics from "../queries/useLoadFeatureViewSummaryStatistics";
import SparklineHistogram from "./SparklineHistogram";

interface FeaturesListProps {
  featureViewName: string;
  features: FeastFeatureColumnType[];
}

const FeaturesList = ({ featureViewName, features }: FeaturesListProps) => {
  const { isLoading, isError, isSuccess, data } =
    useLoadFeatureViewSummaryStatistics(featureViewName);

  let columns: { name: string; render?: any; field: any }[] = [
    { name: "Name", field: "name" },
    {
      name: "Value Type",
      field: "valueType",
    },
  ];

  if (enabledFeatureStatistics) {
    columns.push(
      ...[
        {
          name: "Sample",
          field: "",
          render: (item: FeastFeatureColumnType) => {
            const statistics =
              isSuccess && data && data.columnsSummaryStatistics[item.name];

            return (
              <React.Fragment>
                {isLoading && <EuiLoadingSpinner size="s" />}
                {isError && (
                  <EuiBadge color="warning">error loading samples</EuiBadge>
                )}
                {statistics && statistics.sampleValues.join(",")}
              </React.Fragment>
            );
          },
        },
        {
          name: "Sparklines",
          field: "",
          render: (item: FeastFeatureColumnType) => {
            const statistics =
              isSuccess && data && data.columnsSummaryStatistics[item.name];

            if (
              statistics &&
              statistics.valueType === "INT64" &&
              statistics.histogram
            ) {
              return <SparklineHistogram data={statistics.histogram} />;
            } else {
              return "";
            }
          },
        },
      ]
    );
  }

  const getRowProps = (item: FeastFeatureColumnType) => {
    return {
      "data-test-subj": `row-${item.name}`,
    };
  };

  return (
    <EuiBasicTable columns={columns} items={features} rowProps={getRowProps} />
  );
};

export default FeaturesList;
