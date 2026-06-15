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
  viewType?: string;
}

const FeaturesInServiceList = ({ featureViews }: FeatureViewsListInterace) => {
  const { projectName } = useParams();

  const items: IFeatureColumnInService[] = featureViews.flatMap(
    (featureView: any) =>
      (featureView.featureColumns || []).map((featureColumn: any) => ({
        featureViewName: featureView.featureViewName!,
        name: featureColumn.name!,
        valueType: featureColumn.valueType!,
        viewType: featureView.viewType || "featureView",
      })),
  );

  const isLabelMode = items.length > 0 && items[0].viewType === "labelView";

  const columns = [
    {
      name: isLabelMode ? "Label View" : "Feature View",
      field: "featureViewName",
      render: (name: string, item: IFeatureColumnInService) => {
        const isLabelView = item.viewType === "labelView";
        const path = isLabelView
          ? `/p/${projectName}/label-view/${name}`
          : `/p/${projectName}/feature-view/${name}`;
        return <EuiCustomLink to={path}>{name}</EuiCustomLink>;
      },
    },
    {
      name: isLabelMode ? "Label" : "Feature",
      field: "name",
      render: (name: string, item: IFeatureColumnInService) => {
        const isLabelView = item.viewType === "labelView";
        const path = isLabelView
          ? `/p/${projectName}/label-view/${item.featureViewName}/label/${name}`
          : `/p/${projectName}/feature-view/${item.featureViewName}/feature/${name}`;
        return <EuiCustomLink to={path}>{name}</EuiCustomLink>;
      },
    },
    {
      name: "Value Type",
      field: "valueType",
      render: (valueType: feast.types.ValueType.Enum | string) => {
        if (typeof valueType === "string") return valueType;
        return feast.types.ValueType.Enum[valueType] || String(valueType);
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
