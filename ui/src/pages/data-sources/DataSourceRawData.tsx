import React from "react";
import { EuiPanel } from "@elastic/eui";
import { useParams } from "react-router-dom";
import useLoadDataSource from "./useLoadDataSource";

const DataSourceRawData = () => {
  let { dataSourceName } = useParams();

  const dsName = dataSourceName === undefined ? "" : dataSourceName;

  const { isSuccess, data } = useLoadDataSource(dsName);

  return isSuccess && data ? (
    <EuiPanel hasBorder={true} hasShadow={false}>
      <pre>{JSON.stringify(data, null, 2)}</pre>
    </EuiPanel>
  ) : (
    <EuiPanel hasBorder={true} hasShadow={false}>
      No data so sad
    </EuiPanel>
  );
};

export default DataSourceRawData;
