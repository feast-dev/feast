import React from "react";
import {
  EuiBadge,
  EuiFlexGroup,
  EuiFlexItem,
  EuiPanel,
  EuiText,
  EuiTitle,
  EuiHorizontalRule,
  EuiToolTip,
} from "@elastic/eui";
import { formatPermissions } from "../utils/permissionUtils";

interface PermissionsDisplayProps {
  permissions: any[] | undefined;
}

const PermissionsDisplay: React.FC<PermissionsDisplayProps> = ({
  permissions,
}) => {
  if (!permissions || permissions.length === 0) {
    return (
      <EuiText>
        <p>No permissions defined for this resource.</p>
      </EuiText>
    );
  }

  const getActionColor = (action: string) => {
    if (action.startsWith("READ")) return "success";
    if (action.startsWith("WRITE")) return "warning";
    if (action === "CREATE") return "primary";
    if (action === "UPDATE") return "accent";
    if (action === "DELETE") return "danger";
    return "default";
  };

  return (
    <React.Fragment>
      {permissions.map((permission, index) => {
        const actions = permission.spec?.actions?.map((a: number) => {
          const actionNames = [
            "CREATE",
            "DESCRIBE",
            "UPDATE",
            "DELETE",
            "READ_ONLINE",
            "READ_OFFLINE",
            "WRITE_ONLINE",
            "WRITE_OFFLINE",
          ];
          return actionNames[a] || `Unknown (${a})`;
        });

        return (
          <div key={index} style={{ marginBottom: "8px" }}>
            <EuiToolTip
              position="top"
              content={
                <div>
                  <p>
                    <strong>Name:</strong> {permission.spec?.name}
                  </p>
                  <p>
                    <strong>Policy:</strong>{" "}
                    {permission.spec?.policy?.roles
                      ? `Roles: ${permission.spec.policy.roles.join(", ")}`
                      : "No policy defined"}
                  </p>
                  {permission.spec?.name_patterns && (
                    <p>
                      <strong>Name Patterns:</strong>{" "}
                      {Array.isArray(permission.spec.name_patterns)
                        ? permission.spec.name_patterns.join(", ")
                        : permission.spec.name_patterns}
                    </p>
                  )}
                  {permission.spec?.required_tags && (
                    <p>
                      <strong>Required Tags:</strong>{" "}
                      {Object.entries(permission.spec.required_tags)
                        .map(([key, value]) => `${key}: ${value}`)
                        .join(", ")}
                    </p>
                  )}
                </div>
              }
            >
              <EuiText>
                <h4>{permission.spec?.name}</h4>
              </EuiText>
            </EuiToolTip>
            <EuiFlexGroup wrap responsive={false} gutterSize="xs">
              {actions.map((action: string, actionIndex: number) => (
                <EuiFlexItem grow={false} key={actionIndex}>
                  <EuiBadge color={getActionColor(action)}>{action}</EuiBadge>
                </EuiFlexItem>
              ))}
            </EuiFlexGroup>
          </div>
        );
      })}
    </React.Fragment>
  );
};

export default PermissionsDisplay;
