import React, { useContext } from "react";
import { EuiEmptyPrompt, EuiLoadingSpinner, EuiSpacer } from "@elastic/eui";
import useLoadRegistry from "../queries/useLoadRegistry";
import RegistryPathContext from "../contexts/RegistryPathContext";
import RegistryVisualization from "./RegistryVisualization";

const RegistryVisualizationTab = () => {
  const registryUrl = useContext(RegistryPathContext);
  const { isLoading, isSuccess, isError, data } = useLoadRegistry(registryUrl);

  return (
    <>
      {isLoading && (
        <div style={{ display: "flex", justifyContent: "center", padding: 25 }}>
          <EuiLoadingSpinner size="xl" />
        </div>
      )}
      {isError && (
        <EuiEmptyPrompt
          iconType="alert"
          color="danger"
          title={<h2>Error Loading Registry Data</h2>}
          body={
            <p>
              There was an error loading the Registry Data. Please check that{" "}
              <code>feature_store.yaml</code> file is available and well-formed.
            </p>
          }
        />
      )}
      {isSuccess && data && (
        <>
          <EuiSpacer size="l" />
          <RegistryVisualization
            registryData={data.objects}
            relationships={data.relationships}
            indirectRelationships={data.indirectRelationships}
          />
        </>
      )}
    </>
  );
};

export default RegistryVisualizationTab;
