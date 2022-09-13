import React from "react";
import { EuiBasicTable } from "@elastic/eui";
import EuiCustomLink from "./EuiCustomLink";
import { useParams } from "react-router-dom";
import { feast } from "../protos";

interface FeatureViewsListInterace {
  featureViews: feast.core.IFeatureViewProjection[];
}

const FeaturesInServiceList = ({ featureViews }: FeatureViewsListInterace) => {
  const { projectName } = useParams();

  const columns = [
    {
      name: "Feature View",
      field: "featureViewName",
      render: (name: string) => {
        return (
          <EuiCustomLink
            href={`/p/${projectName}/feature-view/${name}`}
            to={`/p/${projectName}/feature-view/${name}`}
          >
            {name}
          </EuiCustomLink>
        );
      },
    },
    {
      name: "Feature Column",
      field: "featureColumn.name",
    },
    {
      name: "Value Type",
      field: "valueType",
    },
  ];

  const getRowProps = (item: feast.core.IFeatureViewProjection) => {
    return {
      "data-test-subj": `row-${item.featureViewName}`,
    };
  };

  return (
    <EuiBasicTable columns={columns} items={featureViews} rowProps={getRowProps} />
  );
};

export default FeaturesInServiceList;
