import React, { useContext } from "react";
import { EuiBasicTable, EuiLoadingSpinner, EuiBadge, EuiLink } from "@elastic/eui";
import { FeastFeatureColumnType } from "../parsers/feastFeatureViews";
import useLoadFeatureViewSummaryStatistics from "../queries/useLoadFeatureViewSummaryStatistics";
import SparklineHistogram from "./SparklineHistogram";
import FeatureFlagsContext from "../contexts/FeatureFlagsContext";

interface FeaturesListProps {
  projectName: string;
  featureViewName: string;
  features: FeastFeatureColumnType[];
  link: boolean;
}

const FeaturesList = ({ projectName, featureViewName, features, link }: FeaturesListProps) => {
  const { enabledFeatureStatistics } = useContext(FeatureFlagsContext);
  const { isLoading, isError, isSuccess, data } =
    useLoadFeatureViewSummaryStatistics(featureViewName);

  let columns: { name: string; render?: any; field: any }[] = [
    { 
      name: "Name",
      field: "name",
      render: (item: string) => ( 
        <EuiLink href={`/p/${projectName}/feature-view/${featureViewName}/feature/${item}`}>
          {item}
        </EuiLink>
      ) 
    },
    {
      name: "Value Type",
      field: "valueType",
    },
  ];

  if (!link) {
    columns[0].render = undefined;
  }
  
  console.log(columns);

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
