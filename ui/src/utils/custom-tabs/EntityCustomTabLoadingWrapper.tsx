import React from "react";
import { useParams } from "react-router-dom";

import { EntityCustomTabProps } from "../../custom-tabs/types";
import useLoadEntity from "../../pages/entities/useLoadEntity";

interface EntityCustomTabLoadingWrapperProps {
  Component: (props: EntityCustomTabProps) => JSX.Element;
}

const EntityCustomTabLoadingWrapper = ({
  Component,
}: EntityCustomTabLoadingWrapperProps) => {
  let { entityName } = useParams();

  if (!entityName) {
    throw new Error(
      `This route has no 'entityName' part. This route is likely not supposed to render this component.`,
    );
  }

  const feastObjectQuery = useLoadEntity(entityName);

  return <Component id={entityName} feastObjectQuery={feastObjectQuery} />;
};

export default EntityCustomTabLoadingWrapper;
