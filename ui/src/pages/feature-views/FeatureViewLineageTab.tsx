import React, { useContext } from "react";
import { useParams } from "react-router-dom";
import { EuiEmptyPrompt, EuiLoadingSpinner } from "@elastic/eui";
import { feast } from "../../protos";
import useLoadRegistry from "../../queries/useLoadRegistry";
import RegistryPathContext from "../../contexts/RegistryPathContext";
import RegistryVisualization from "../../components/RegistryVisualization";
import { FEAST_FCO_TYPES } from "../../parsers/types";

interface FeatureViewLineageTabProps {
  data: feast.core.IFeatureView;
}

const FeatureViewLineageTab = ({ data }: FeatureViewLineageTabProps) => {
  const registryUrl = useContext(RegistryPathContext);
  const {
    isLoading,
    isSuccess,
    isError,
    data: registryData,
  } = useLoadRegistry(registryUrl);
  const { featureViewName } = useParams();

  const filterNode = {
    type: FEAST_FCO_TYPES.featureView,
    name: featureViewName || data.spec?.name || "",
  };

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
      {isSuccess && registryData && (
        <RegistryVisualization
          registryData={registryData.objects}
          relationships={registryData.relationships}
          indirectRelationships={registryData.indirectRelationships}
          filterNode={filterNode}
        />
      )}
    </>
  );
};

export default FeatureViewLineageTab;
