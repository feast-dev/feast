import React, { useContext, useState } from "react";
import { useParams } from "react-router-dom";
import {
  EuiEmptyPrompt,
  EuiLoadingSpinner,
  EuiSpacer,
  EuiSelect,
  EuiFormRow,
  EuiFlexGroup,
  EuiFlexItem,
} from "@elastic/eui";
import { feast } from "../../protos";
import useLoadRegistry from "../../queries/useLoadRegistry";
import RegistryPathContext from "../../contexts/RegistryPathContext";
import RegistryVisualization from "../../components/RegistryVisualization";
import { FEAST_FCO_TYPES } from "../../parsers/types";
import { filterPermissionsByAction } from "../../utils/permissionUtils";

interface FeatureViewLineageTabProps {
  data: feast.core.IFeatureView;
}

const FeatureViewLineageTab = ({ data }: FeatureViewLineageTabProps) => {
  const registryUrl = useContext(RegistryPathContext);
  const { featureViewName, projectName } = useParams();
  const {
    isLoading,
    isSuccess,
    isError,
    data: registryData,
  } = useLoadRegistry(registryUrl, projectName);
  const [selectedPermissionAction, setSelectedPermissionAction] = useState("");

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
        <>
          <EuiSpacer size="l" />
          <EuiFlexGroup style={{ marginBottom: 16 }}>
            <EuiFlexItem grow={false} style={{ width: 300 }}>
              <EuiFormRow label="Filter by permissions">
                <EuiSelect
                  options={[
                    { value: "", text: "All" },
                    { value: "CREATE", text: "CREATE" },
                    { value: "DESCRIBE", text: "DESCRIBE" },
                    { value: "UPDATE", text: "UPDATE" },
                    { value: "DELETE", text: "DELETE" },
                    { value: "READ_ONLINE", text: "READ_ONLINE" },
                    { value: "READ_OFFLINE", text: "READ_OFFLINE" },
                    { value: "WRITE_ONLINE", text: "WRITE_ONLINE" },
                    { value: "WRITE_OFFLINE", text: "WRITE_OFFLINE" },
                  ]}
                  value={selectedPermissionAction}
                  onChange={(e) => setSelectedPermissionAction(e.target.value)}
                  aria-label="Filter by permissions"
                />
              </EuiFormRow>
            </EuiFlexItem>
          </EuiFlexGroup>
          <RegistryVisualization
            registryData={registryData.objects}
            relationships={registryData.relationships}
            indirectRelationships={registryData.indirectRelationships}
            permissions={
              selectedPermissionAction
                ? filterPermissionsByAction(
                    registryData.permissions,
                    selectedPermissionAction,
                  )
                : registryData.permissions
            }
            filterNode={filterNode}
          />
        </>
      )}
    </>
  );
};

export default FeatureViewLineageTab;
