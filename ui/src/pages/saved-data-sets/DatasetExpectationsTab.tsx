import React from "react";
import { EuiPanel } from "@elastic/eui";
import { useParams } from "react-router-dom";
import useLoadDataset from "./useLoadDataset";

const DatasetExpectationsTab = () => {
  let { datasetName } = useParams();

  if (!datasetName) {
    throw new Error("Unable to get dataset name.");
  }
  const { isSuccess, data } = useLoadDataset(datasetName);

  if (!data || !data.spec?.name) {
    return (
      <EuiPanel hasBorder={true} hasShadow={false}>
        No data so sad
      </EuiPanel>
    );
  }

  let expectationsData;

  return isSuccess ? (
    <EuiPanel hasBorder={true} hasShadow={false}>
      <pre>{JSON.stringify(data.spec, null, 2)}</pre>
    </EuiPanel>
  ) : (
    <EuiPanel hasBorder={true} hasShadow={false}>
      No data so sad
    </EuiPanel>
  );
};

export default DatasetExpectationsTab;
