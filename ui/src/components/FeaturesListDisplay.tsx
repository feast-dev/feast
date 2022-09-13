import React, { useContext } from "react";
import { EuiBasicTable, EuiLoadingSpinner, EuiBadge } from "@elastic/eui";
import useLoadFeatureViewSummaryStatistics from "../queries/useLoadFeatureViewSummaryStatistics";
import SparklineHistogram from "./SparklineHistogram";
import FeatureFlagsContext from "../contexts/FeatureFlagsContext";
import EuiCustomLink from "./EuiCustomLink";
import { feast } from "../protos";

interface FeaturesListProps {
  projectName: string;
  featureViewName: string;
  features: feast.core.IFeatureSpecV2[];
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
        <EuiCustomLink
          href={`/p/${projectName}/feature-view/${featureViewName}/feature/${item}`}
          to={`/p/${projectName}/feature-view/${featureViewName}/feature/${item}`}>
          {item}
        </EuiCustomLink>
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

  if (enabledFeatureStatistics) {
    columns.push(
      ...[
        {
          name: "Sample",
          field: "",
          render: (item: feast.core.IFeatureSpecV2) => {
            const statistics =
              isSuccess && data && data.columnsSummaryStatistics[item.name!];

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
          render: (item: feast.core.IFeatureSpecV2) => {
            const statistics =
              isSuccess && data && data.columnsSummaryStatistics[item?.name!];

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

  const getRowProps = (item: feast.core.IFeatureSpecV2) => {
    return {
      "data-test-subj": `row-${item.name}`,
    };
  };

  return (
    <EuiBasicTable columns={columns} items={features} rowProps={getRowProps} />
  );
};

export default FeaturesList;
