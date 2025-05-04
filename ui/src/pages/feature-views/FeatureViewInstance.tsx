import React from "react";

import { useParams } from "react-router-dom";
import { EuiLoadingSpinner } from "@elastic/eui";

import RegularFeatureInstance from "./RegularFeatureViewInstance";
import { FEAST_FV_TYPES } from "../../parsers/mergedFVTypes";

import useLoadFeatureView from "./useLoadFeatureView";
import OnDemandFeatureInstance from "./OnDemandFeatureViewInstance";
import StreamFeatureInstance from "./StreamFeatureViewInstance";
import { feast } from "../../protos";
import useLoadRegistry from "../../queries/useLoadRegistry";
import RegistryPathContext from "../../contexts/RegistryPathContext";

const FeatureViewInstance = () => {
  const { featureViewName } = useParams();
  const registryUrl = React.useContext(RegistryPathContext);
  const registryQuery = useLoadRegistry(registryUrl);

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
      const fv: feast.core.IFeatureView = data.object;

      return <RegularFeatureInstance data={fv} permissions={registryQuery.data?.permissions} />;
    }

    if (data.type === FEAST_FV_TYPES.ondemand) {
      const odfv: feast.core.IOnDemandFeatureView = data.object;

      return <OnDemandFeatureInstance data={odfv} />;
    }
    if (data.type === FEAST_FV_TYPES.stream) {
      const sfv: feast.core.IStreamFeatureView = data.object;

      return <StreamFeatureInstance data={sfv} />;
    }
  }

  return <p>No Data So Sad</p>;
};

export default FeatureViewInstance;
