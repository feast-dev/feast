import React from "react";
import { EuiBasicTable, EuiPanel, EuiText, EuiTitle } from "@elastic/eui";

import { useParams } from "react-router-dom";
import EuiCustomLink from "../../../components/EuiCustomLink";
import { feast } from "../../../protos";

interface RequestDataDisplayPanelProps extends feast.core.IFeatureViewProjection { }

const FeatureViewProjectionDisplayPanel = (featureViewProjection: RequestDataDisplayPanelProps) => {
  const { projectName } = useParams();

  const columns = [
    {
      name: "Column Name",
      field: "name"
    },
    {
      name: "Type",
      field: "valueType",
      render: (valueType: any) => {
        return feast.types.ValueType.Enum[valueType];
      },
    },
  ];

  return (
    <EuiPanel hasBorder={true}>
      <EuiText size="xs">
        <span>Feature View</span>
      </EuiText>
      <EuiTitle size="s">
        <EuiCustomLink
          href={`/p/${projectName}/feature-view/${featureViewProjection.featureViewName}`}
          to={`/p/${projectName}/feature-view/${featureViewProjection.featureViewName}`}
        >
          {featureViewProjection?.featureViewName}
        </EuiCustomLink>
      </EuiTitle>
      <EuiBasicTable
        columns={columns}
        items={featureViewProjection?.featureColumns!}
      />
    </EuiPanel>
  );
};

export default FeatureViewProjectionDisplayPanel;
