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

  if (!data || !data.spec.profile) {
    return (
      <EuiPanel hasBorder={true} hasShadow={false}>
        No data so sad
      </EuiPanel>
    );
  }

  let expectationsData;

  try {
    expectationsData = JSON.parse(data.spec.profile);
  } catch (e) {
    throw new Error(`Unable to parse Expectations Profile: ${e}`);
  }

  return isSuccess && expectationsData ? (
    <EuiPanel hasBorder={true} hasShadow={false}>
      <pre>{JSON.stringify(expectationsData, null, 2)}</pre>
    </EuiPanel>
  ) : (
    <EuiPanel hasBorder={true} hasShadow={false}>
      No data so sad
    </EuiPanel>
  );
};

export default DatasetExpectationsTab;
