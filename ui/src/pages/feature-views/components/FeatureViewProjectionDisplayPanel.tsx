import React from "react";
import {
  EuiBadge,
  EuiBasicTable,
  EuiPanel,
  EuiSpacer,
  EuiTitle,
} from "@elastic/eui";
import { useParams } from "react-router-dom";
import EuiCustomLink from "../../../components/EuiCustomLink";
import { feast } from "../../../protos";

interface RequestDataDisplayPanelProps
  extends feast.core.IFeatureViewProjection {}

const FeatureViewProjectionDisplayPanel = (
  featureViewProjection: RequestDataDisplayPanelProps,
) => {
  const { projectName } = useParams();
  const isLabelView = (featureViewProjection as any).viewType === "labelView";
  const viewPath = isLabelView ? "label-view" : "feature-view";

  const columns = [
    {
      name: "Column Name",
      field: "name",
    },
    {
      name: "Type",
      field: "valueType",
      render: (valueType: any) => {
        if (typeof valueType === "string") return valueType;
        return feast.types.ValueType.Enum[valueType] || String(valueType || "");
      },
    },
  ];

  return (
    <EuiPanel hasBorder={true}>
      <EuiBadge color={isLabelView ? "#e6570e" : "hollow"}>
        {isLabelView ? "label view" : "feature view"}
      </EuiBadge>
      <EuiSpacer size="xs" />
      <EuiTitle size="s">
        <EuiCustomLink
          to={`/p/${projectName}/${viewPath}/${featureViewProjection.featureViewName}`}
        >
          {featureViewProjection?.featureViewName}
        </EuiCustomLink>
      </EuiTitle>
      <EuiSpacer size="s" />
      <EuiBasicTable
        columns={columns}
        items={featureViewProjection?.featureColumns || []}
      />
    </EuiPanel>
  );
};

export default FeatureViewProjectionDisplayPanel;
