import React from "react";

import { useParams } from "react-router-dom";
import useLoadFeatureView from "../../pages/feature-views/useLoadFeatureView";
import {
  OnDemandFeatureViewCustomTabProps,
  OnDemandFeatureViewQueryReturnType,
} from "../../custom-tabs/types";
import { FEAST_FV_TYPES } from "../../parsers/mergedFVTypes";

interface OnDemandFeatureViewCustomTabLoadingWrapperProps {
  Component: (props: OnDemandFeatureViewCustomTabProps) => JSX.Element;
}

const OnDemandFeatureViewCustomTabLoadingWrapper = ({
  Component,
}: OnDemandFeatureViewCustomTabLoadingWrapperProps) => {
  const { featureViewName } = useParams();

  if (!featureViewName) {
    throw new Error(
      `This route has no 'featureViewName' part. This route is likely not supposed to render this component.`
    );
  }

  const feastObjectQuery = useLoadFeatureView(featureViewName);

  if (
    feastObjectQuery.isSuccess &&
    feastObjectQuery.data &&
    feastObjectQuery.data.type !== FEAST_FV_TYPES.ondemand
  ) {
    throw new Error(
      `This should not happen. Somehow a custom tab on a ODFV page received data that does not have the shape?`
    );
  }

  return (
    <Component
      id={featureViewName}
      feastObjectQuery={feastObjectQuery as OnDemandFeatureViewQueryReturnType}
    />
  );
};

export default OnDemandFeatureViewCustomTabLoadingWrapper;
