import React from "react";
import { useParams } from "react-router-dom";

import { DataSourceCustomTabProps } from "../../custom-tabs/types";
import useLoadDataSource from "../../pages/data-sources/useLoadDataSource";

interface DataSourceCustomTabLoadingWrapperProps {
  Component: (props: DataSourceCustomTabProps) => JSX.Element;
}

const DataSourceCustomTabLoadingWrapper = ({
  Component,
}: DataSourceCustomTabLoadingWrapperProps) => {
  let { dataSourceName } = useParams();

  if (!dataSourceName) {
    throw new Error(
      `This route has no 'dataSourceName' part. This route is likely not supposed to render this component.`
    );
  }

  const feastObjectQuery = useLoadDataSource(dataSourceName);

  return <Component id={dataSourceName} feastObjectQuery={feastObjectQuery} />;
};

export default DataSourceCustomTabLoadingWrapper;
