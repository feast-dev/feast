import React from "react";
import { EuiBasicTable, EuiPanel, EuiText, EuiTitle } from "@elastic/eui";
import { useParams } from "react-router-dom";
import EuiCustomLink from "../../../components/EuiCustomLink";
import { feast } from "../../../protos";

interface RequestDataDisplayPanelProps extends feast.core.IOnDemandSource { }

const RequestDataDisplayPanel = ({
  requestDataSource,
}: RequestDataDisplayPanelProps) => {
  const { projectName } = useParams();

  const items = Object.entries(requestDataSource?.requestDataOptions?.schema!).map(
    ([key, type]) => {
      return {
        key,
        type,
      };
    }
  );

  const columns = [
    {
      name: "Key",
      field: "key",
    },
    {
      name: "Type",
      field: "type",
    },
  ];

  return (
    <EuiPanel hasBorder={true}>
      <EuiText size="xs">
        <span>Request Data</span>
      </EuiText>
      <EuiTitle size="s">
        <EuiCustomLink
          href={`${process.env.PUBLIC_URL || ""}/p/${projectName}/data-source/${requestDataSource?.name}`}
          to={`${process.env.PUBLIC_URL || ""}/p/${projectName}/data-source/${requestDataSource?.name}`}
        >
          {requestDataSource?.name}
        </EuiCustomLink>
      </EuiTitle>
      <EuiBasicTable columns={columns} items={items} />
    </EuiPanel>
  );
};

export default RequestDataDisplayPanel;
