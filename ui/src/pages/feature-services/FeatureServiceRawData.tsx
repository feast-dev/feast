import React from "react";
import { EuiPanel } from "@elastic/eui";
import { useParams } from "react-router-dom";
import useLoadFeatureService from "./useLoadFeatureService";

const FeatureServiceRawData = () => {
  let { featureServiceName } = useParams();

  const fsName = featureServiceName === undefined ? "" : featureServiceName;

  const { isSuccess, data } = useLoadFeatureService(fsName);

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

export default FeatureServiceRawData;
