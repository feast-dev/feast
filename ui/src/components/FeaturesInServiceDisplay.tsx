import React from "react";
import { EuiBasicTable } from "@elastic/eui";
import EuiCustomLink from "./EuiCustomLink";
import { useParams } from "react-router-dom";
import { feast } from "../protos";

interface FeatureViewsListInterace {
  featureViews: feast.core.IFeatureViewProjection[];
}

interface IFeatureColumnInService {
  featureViewName: string;
  name: string;
  valueType: feast.types.ValueType.Enum;
}

const FeaturesInServiceList = ({ featureViews }: FeatureViewsListInterace) => {
  const { projectName } = useParams();
  const items: IFeatureColumnInService[] = featureViews.flatMap(featureView => featureView.featureColumns!.map(featureColumn => ({
    featureViewName: featureView.featureViewName!,
    name: featureColumn.name!,
    valueType: featureColumn.valueType!,
  })));

  const columns = [
    {
      name: "Feature View",
      field: "featureViewName",
      render: (name: string) => {
        return (
          <EuiCustomLink to={`/p/${projectName}/feature-view/${name}`}>
            {name}
          </EuiCustomLink>
        );
      },
    },
    {
      name: "Feature Column",
      field: "name",
    },
    {
      name: "Value Type",
      field: "valueType",
      render: (valueType: feast.types.ValueType.Enum) => {
        return feast.types.ValueType.Enum[valueType];
      },
    },
  ];

  const getRowProps = (item: IFeatureColumnInService) => {
    return {
      "data-test-subj": `row-${item.featureViewName}`,
    };
  };

  return (
    <EuiBasicTable columns={columns} items={items} rowProps={getRowProps} />
  );
};

export default FeaturesInServiceList;
