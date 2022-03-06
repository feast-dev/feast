import React from "react";

import { useParams } from "react-router-dom";
import { EuiLoadingSpinner } from "@elastic/eui";

import { FeastFeatureViewType } from "../../parsers/feastFeatureViews";
import RegularFeatureInstance from "./RegularFeatureViewInstance";
import { FEAST_FV_TYPES } from "../../parsers/mergedFVTypes";
import { FeastODFVType } from "../../parsers/feastODFVS";
import useLoadFeatureView from "./useLoadFeatureView";
import OnDemandFeatureInstance from "./OnDemandFeatureViewInstance";

const FeatureViewInstance = () => {
  const { featureViewName } = useParams();

  const fvName = featureViewName === undefined ? "" : featureViewName;

  const { isLoading, isSuccess, isError, data } = useLoadFeatureView(fvName);
  const isEmpty = data === undefined;

  if (isLoading) {
    return (
      <React.Fragment>
        <EuiLoadingSpinner size="m" /> Loading
      </React.Fragment>
    );
  }
  if (isEmpty) {
    return <p>No feature view with name: {featureViewName}</p>;
  }

  if (isError) {
    isError && <p>Error loading feature view: {featureViewName}</p>;
  }

  if (isSuccess && !isEmpty) {
    if (data.type === FEAST_FV_TYPES.regular) {
      const fv: FeastFeatureViewType = data.object;

      return <RegularFeatureInstance data={fv} />;
    }

    if (data.type === FEAST_FV_TYPES.ondemand) {
      const odfv: FeastODFVType = data.object;

      return <OnDemandFeatureInstance data={odfv} />;
    }
  }

  return <p>No Data So Sad</p>;
};

export default FeatureViewInstance;
