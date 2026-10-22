import React from "react";
import { EuiPanel } from "@elastic/eui";
import { useParams } from "react-router-dom";
import useLoadEntity from "./useLoadEntity";

const EntityRawData = () => {
  let { entityName } = useParams();

  const eName = entityName === undefined ? "" : entityName;

  const { isSuccess, data } = useLoadEntity(eName);

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

export default EntityRawData;
