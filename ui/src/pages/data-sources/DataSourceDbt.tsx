import React from "react";
import { 
  EuiCodeBlock,
  EuiPanel,  
  EuiHorizontalRule,
  EuiTitle,
  } from "@elastic/eui";
import { useParams } from "react-router";
import useLoadDataSource from "./useLoadDataSource";

const DataSourceDbt = () => {
  let { dataSourceName } = useParams();

  const dsName = dataSourceName === undefined ? "" : dataSourceName;

  const { isSuccess, data } = useLoadDataSource(dsName);

  return isSuccess && data && data.bigqueryOptions ? (
    <EuiPanel hasBorder={true} hasShadow={false}>
      <EuiTitle size="s">
        <h3>Dbt Transformation</h3>
      </EuiTitle>
      <EuiHorizontalRule margin="xs" />
      <EuiCodeBlock language="sql" fontSize="m" paddingSize="m" isCopyable>
        {data.bigqueryOptions.dbtModelSerialized}
      </EuiCodeBlock>
    </EuiPanel>
  ) : (
    <EuiPanel hasBorder={true} hasShadow={false}>No data so sad</EuiPanel>
  );
};

export default DataSourceDbt;
