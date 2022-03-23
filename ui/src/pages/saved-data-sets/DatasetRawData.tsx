import React from "react";
import { EuiPanel } from "@elastic/eui";
import { useParams } from "react-router-dom";
import useLoadDataset from "./useLoadDataset";

const EntityRawData = () => {
  let { datasetName } = useParams();

  if (!datasetName) {
    throw new Error("Unable to get dataset name.");
  }
  const { isSuccess, data } = useLoadDataset(datasetName);

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

export default EntityRawData;
