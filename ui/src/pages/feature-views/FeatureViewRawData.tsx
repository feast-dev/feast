import React from "react";
import { EuiPanel } from "@elastic/eui";
import { useParams } from "react-router-dom";
import useLoadFeatureView from "./useLoadFeatureView";

const FeatureViewRawData = () => {
  let { featureViewName } = useParams();

  const fvName = featureViewName === undefined ? "" : featureViewName;

  const { isSuccess, data } = useLoadFeatureView(fvName);

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

export default FeatureViewRawData;
