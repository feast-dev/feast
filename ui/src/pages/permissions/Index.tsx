import React from "react";
import {
  EuiPageTemplate,
  EuiTitle,
  EuiSpacer,
  EuiPanel,
  EuiFlexGroup,
  EuiFlexItem,
  EuiText,
  EuiLoadingSpinner,
  EuiHorizontalRule,
  EuiSelect,
  EuiFormRow,
} from "@elastic/eui";
import { useContext, useState } from "react";
import RegistryPathContext from "../../contexts/RegistryPathContext";
import useLoadRegistry from "../../queries/useLoadRegistry";
import PermissionsDisplay from "../../components/PermissionsDisplay";
import { filterPermissionsByAction } from "../../utils/permissionUtils";

const PermissionsIndex = () => {
  const registryUrl = useContext(RegistryPathContext);
  const { isLoading, isSuccess, isError, data } = useLoadRegistry(registryUrl);
  const [selectedPermissionAction, setSelectedPermissionAction] = useState("");

  return (
    <EuiPageTemplate restrictWidth>
      <EuiPageTemplate.Header
        pageTitle="Permissions"
        description="View and manage permissions for Feast resources"
      />
      <EuiPageTemplate.Section>
        {isLoading && (
          <React.Fragment>
            <EuiLoadingSpinner size="m" /> Loading
          </React.Fragment>
        )}
        {isError && <p>Error loading permissions</p>}
        {isSuccess && data && (
          <React.Fragment>
            <EuiFlexGroup>
              <EuiFlexItem grow={false} style={{ width: 300 }}>
                <EuiFormRow label="Filter by action">
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
                    onChange={(e) =>
                      setSelectedPermissionAction(e.target.value)
                    }
                    aria-label="Filter by action"
                  />
                </EuiFormRow>
              </EuiFlexItem>
            </EuiFlexGroup>
            <EuiSpacer size="m" />
            <EuiPanel hasBorder={true}>
              <EuiTitle size="s">
                <h2>Permissions</h2>
              </EuiTitle>
              <EuiHorizontalRule margin="xs" />
              {data.permissions && data.permissions.length > 0 ? (
                <PermissionsDisplay
                  permissions={
                    selectedPermissionAction
                      ? filterPermissionsByAction(
                          data.permissions,
                          selectedPermissionAction,
                        )
                      : data.permissions
                  }
                />
              ) : (
                <EuiText>No permissions defined in this project.</EuiText>
              )}
            </EuiPanel>
          </React.Fragment>
        )}
      </EuiPageTemplate.Section>
    </EuiPageTemplate>
  );
};

export default PermissionsIndex;
