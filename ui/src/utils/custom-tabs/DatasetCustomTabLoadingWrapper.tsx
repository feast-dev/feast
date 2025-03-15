import React from "react";
import { useParams } from "react-router-dom";

import { DatasetCustomTabProps } from "../../custom-tabs/types";
import useLoadDataset from "../../pages/saved-data-sets/useLoadDataset";

interface DatasetCustomTabLoadingWrapperProps {
  Component: (props: DatasetCustomTabProps) => JSX.Element;
}

const DatasetCustomTabLoadingWrapper = ({
  Component,
}: DatasetCustomTabLoadingWrapperProps) => {
  let { datasetName } = useParams();

  if (!datasetName) {
    throw new Error(
      "Route doesn't have a 'datasetName' part. This route is likely rendering the wrong component.",
    );
  }

  const feastObjectQuery = useLoadDataset(datasetName);

  return <Component id={datasetName} feastObjectQuery={feastObjectQuery} />;
};

export default DatasetCustomTabLoadingWrapper;
