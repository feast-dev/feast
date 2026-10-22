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
import useLoadRegistry from "../../queries/useLoadRegistry";
import RegistryPathContext from "../../contexts/RegistryPathContext";
import RegistryVisualization from "../../components/RegistryVisualization";
import { FEAST_FCO_TYPES } from "../../parsers/types";
import { filterPermissionsByAction } from "../../utils/permissionUtils";

const LabelViewLineageTab = () => {
  const registryUrl = useContext(RegistryPathContext);
  const { labelViewName, projectName } = useParams();
  const {
    isLoading,
    isSuccess,
    isError,
    data: registryData,
  } = useLoadRegistry(registryUrl, projectName);
  const [selectedPermissionAction, setSelectedPermissionAction] = useState("");

  const filterNode = {
    type: FEAST_FCO_TYPES.labelView,
    name: labelViewName || "",
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
          title={<h2>Error loading lineage</h2>}
          body={<p>Could not load lineage data for this label view.</p>}
        />
      )}
      {isSuccess && registryData && (
        <>
          <EuiFlexGroup>
            <EuiFlexItem grow={false} style={{ width: 250 }}>
              <EuiFormRow label="Filter by permission">
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
          <EuiSpacer size="m" />
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

export default LabelViewLineageTab;
