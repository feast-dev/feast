import React from "react";

import { useParams } from "react-router-dom";
import useLoadFeatureView from "../../pages/feature-views/useLoadFeatureView";
import {
  StreamFeatureViewCustomTabProps,
  StreamFeatureViewQueryReturnType,
} from "../../custom-tabs/types";
import { FEAST_FV_TYPES } from "../../parsers/mergedFVTypes";

interface StreamFeatureViewCustomTabLoadingWrapperProps {
  Component: (props: StreamFeatureViewCustomTabProps) => JSX.Element;
}

const StreamFeatureViewCustomTabLoadingWrapper = ({
  Component,
}: StreamFeatureViewCustomTabLoadingWrapperProps) => {
  const { featureViewName } = useParams();

  if (!featureViewName) {
    throw new Error(
      `This route has no 'featureViewName' part. This route is likely not supposed to render this component.`,
    );
  }

  const feastObjectQuery = useLoadFeatureView(featureViewName);

  if (
    feastObjectQuery.isSuccess &&
    feastObjectQuery.data &&
    feastObjectQuery.data.type !== FEAST_FV_TYPES.stream
  ) {
    throw new Error(
      `This should not happen. Somehow a custom tab on a SFV page received data that does not have the shape?`,
    );
  }

  return (
    <Component
      id={featureViewName}
      feastObjectQuery={feastObjectQuery as StreamFeatureViewQueryReturnType}
    />
  );
};

export default StreamFeatureViewCustomTabLoadingWrapper;
