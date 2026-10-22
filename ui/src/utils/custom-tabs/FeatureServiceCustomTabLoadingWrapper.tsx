import React from "react";
import { useParams } from "react-router-dom";

import { FeatureServiceCustomTabProps } from "../../custom-tabs/types";
import useLoadFeatureService from "../../pages/feature-services/useLoadFeatureService";

interface FeatureServiceCustomTabLoadingWrapperProps {
  Component: (props: FeatureServiceCustomTabProps) => JSX.Element;
}

const FeatureServiceCustomTabLoadingWrapper = ({
  Component,
}: FeatureServiceCustomTabLoadingWrapperProps) => {
  const { featureServiceName } = useParams();

  if (!featureServiceName) {
    throw new Error(
      `This route has no 'featureServiceName' part. This route is likely not supposed to render this component.`,
    );
  }

  const feastObjectQuery = useLoadFeatureService(featureServiceName);

  return (
    <Component id={featureServiceName} feastObjectQuery={feastObjectQuery} />
  );
};

export default FeatureServiceCustomTabLoadingWrapper;
