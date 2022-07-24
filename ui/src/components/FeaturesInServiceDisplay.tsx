import React from "react";
import { z } from "zod";
import { EuiBasicTable } from "@elastic/eui";
import { FeastFeatureInServiceType } from "../parsers/feastFeatureServices";
import EuiCustomLink from "./EuiCustomLink";
import { FEAST_FEATURE_VALUE_TYPES } from "../parsers/types";
import { useParams } from "react-router-dom";

interface FeatureViewsListInterace {
  featureViews: FeastFeatureInServiceType[];
}

const FeaturesInServiceList = ({ featureViews }: FeatureViewsListInterace) => {
  const { projectName } = useParams();

  const FeatureInService = z.object({
    featureViewName: z.string(),
    featureColumnName: z.string(),
    valueType: z.nativeEnum(FEAST_FEATURE_VALUE_TYPES),
  });
  type FeatureInServiceType = z.infer<typeof FeatureInService>;

  var items: FeatureInServiceType[] = [];
  featureViews.forEach((featureView) => {
    featureView.featureColumns.forEach((featureColumn) => {
      const row: FeatureInServiceType = {
        featureViewName: featureView.featureViewName,
        featureColumnName: featureColumn.name,
        valueType: featureColumn.valueType,
      };
      items.push(row);
    });
  });

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
      field: "featureColumnName",
    },
    {
      name: "Value Type",
      field: "valueType",
    },
  ];

  const getRowProps = (item: FeatureInServiceType) => {
    return {
      "data-test-subj": `row-${item.featureViewName}`,
    };
  };

  return (
    <EuiBasicTable columns={columns} items={items} rowProps={getRowProps} />
  );
};

export default FeaturesInServiceList;
