import React from "react";
import { useParams } from "react-router-dom";

import { FeatureCustomTabProps } from "../../custom-tabs/types";
import useLoadFeature from "../../pages/features/useLoadFeature";

interface FeatureCustomTabLoadingWrapperProps {
  Component: (props: FeatureCustomTabProps) => JSX.Element;
}

const FeatureCustomTabLoadingWrapper = ({
  Component,
}: FeatureCustomTabLoadingWrapperProps) => {
  console.log(useParams());
  const { FeatureViewName, FeatureName } = useParams();

  if (!FeatureViewName) {
    throw new Error(
      `This route has no 'FeatureViewName' part. This route is likely not supposed to render this component.`
    );
  }

  if (!FeatureName) {
    throw new Error(
      `This route has no 'FeatureName' part. This route is likely not supposed to render this component.`
    );
  }

  const feastObjectQuery = useLoadFeature(FeatureViewName, FeatureName);

  // do I include FeatureViewName in this?
  return (
    <Component id={FeatureName} feastObjectQuery={feastObjectQuery} />
  );
};

export default FeatureCustomTabLoadingWrapper;
