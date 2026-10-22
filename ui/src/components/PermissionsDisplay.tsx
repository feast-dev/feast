import React from "react";
import {
  EuiBadge,
  EuiFlexGroup,
  EuiFlexItem,
  EuiText,
  EuiToolTip,
  EuiPanel,
  EuiSpacer,
  EuiHorizontalRule,
} from "@elastic/eui";

interface PermissionsDisplayProps {
  permissions: any[] | undefined;
}

const ACTION_NAMES = [
  "CREATE",
  "DESCRIBE",
  "UPDATE",
  "DELETE",
  "READ_ONLINE",
  "READ_OFFLINE",
  "WRITE_ONLINE",
  "WRITE_OFFLINE",
];

const getActionColor = (action: string) => {
  if (action.startsWith("READ")) return "success";
  if (action.startsWith("WRITE")) return "warning";
  if (action === "CREATE") return "primary";
  if (action === "UPDATE") return "accent";
  if (action === "DELETE") return "danger";
  if (action === "DESCRIBE") return "hollow";
  return "default";
};

const getPolicyLabel = (policy: any): string => {
  if (!policy) return "Allow All";
  if (policy.roleBasedPolicy?.roles) {
    return `Roles: ${policy.roleBasedPolicy.roles.join(", ")}`;
  }
  if (policy.groupBasedPolicy?.groups) {
    return `Groups: ${policy.groupBasedPolicy.groups.join(", ")}`;
  }
  if (policy.namespaceBasedPolicy?.namespaces) {
    return `Namespaces: ${policy.namespaceBasedPolicy.namespaces.join(", ")}`;
  }
  if (policy.combinedGroupNamespacePolicy) {
    const parts = [];
    if (policy.combinedGroupNamespacePolicy.groups?.length) {
      parts.push(
        `Groups: ${policy.combinedGroupNamespacePolicy.groups.join(", ")}`,
      );
    }
    if (policy.combinedGroupNamespacePolicy.namespaces?.length) {
      parts.push(
        `Namespaces: ${policy.combinedGroupNamespacePolicy.namespaces.join(", ")}`,
      );
    }
    return parts.join(" | ") || "Allow All";
  }
  return "Allow All";
};

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

  return (
    <React.Fragment>
      {permissions.map((permission, index) => {
        const actions = (permission.spec?.actions || []).map(
          (a: number) => ACTION_NAMES[a] || `Unknown (${a})`,
        );
        const policy = permission.spec?.policy;
        const policyLabel = getPolicyLabel(policy);
        const namePatterns =
          permission.spec?.namePatterns || permission.spec?.name_patterns || [];
        const requiredTags =
          permission.spec?.requiredTags || permission.spec?.required_tags || {};

        return (
          <React.Fragment key={index}>
            {index > 0 && <EuiHorizontalRule margin="s" />}
            <EuiPanel paddingSize="s" hasBorder={false} hasShadow={false}>
              <EuiText>
                <h4>{permission.spec?.name}</h4>
              </EuiText>
              <EuiSpacer size="xs" />
              <EuiFlexGroup wrap responsive={false} gutterSize="xs">
                {actions.map((action: string, actionIndex: number) => (
                  <EuiFlexItem grow={false} key={actionIndex}>
                    <EuiBadge color={getActionColor(action)}>{action}</EuiBadge>
                  </EuiFlexItem>
                ))}
              </EuiFlexGroup>
              <EuiSpacer size="xs" />
              <EuiToolTip
                position="bottom"
                content={
                  <div>
                    <p>
                      <strong>Policy:</strong> {policyLabel}
                    </p>
                    {namePatterns.length > 0 && (
                      <p>
                        <strong>Name Patterns:</strong>{" "}
                        {namePatterns.join(", ")}
                      </p>
                    )}
                    {Object.keys(requiredTags).length > 0 && (
                      <p>
                        <strong>Required Tags:</strong>{" "}
                        {Object.entries(requiredTags)
                          .map(([key, value]) => `${key}: ${value}`)
                          .join(", ")}
                      </p>
                    )}
                  </div>
                }
              >
                <EuiText size="xs" color="subdued">
                  {policyLabel}
                </EuiText>
              </EuiToolTip>
            </EuiPanel>
          </React.Fragment>
        );
      })}
    </React.Fragment>
  );
};

export default PermissionsDisplay;
