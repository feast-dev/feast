import React from "react";
import { EuiBasicTable, EuiPanel, EuiSpacer, EuiText, EuiTitle } from "@elastic/eui";
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
      <EuiSpacer size="xs" />
      <EuiTitle size="s">
        <EuiCustomLink
          to={`/p/${projectName}/data-source/${requestDataSource?.name}`}
        >
          {requestDataSource?.name}
        </EuiCustomLink>
      </EuiTitle>
      <EuiSpacer size="s" />
      <EuiBasicTable columns={columns} items={items} />
    </EuiPanel>
  );
};

export default RequestDataDisplayPanel;
