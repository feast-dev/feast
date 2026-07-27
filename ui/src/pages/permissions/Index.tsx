import React, { useState, useMemo } from "react";
import {
  EuiPageTemplate,
  EuiSpacer,
  EuiLoadingSpinner,
  EuiFlexGroup,
  EuiFlexItem,
  EuiButton,
  EuiCallOut,
  EuiFieldSearch,
  EuiTitle,
  EuiBasicTable,
  EuiBadge,
  EuiButtonIcon,
  EuiConfirmModal,
  EuiText,
  EuiToolTip,
  EuiSelect,
  EuiFormRow,
  EuiEmptyPrompt,
} from "@elastic/eui";
import { useParams } from "react-router-dom";
import { PermissionsIcon } from "../../graphics/PermissionsIcon";
import PermissionFormModal, {
  PermissionFormData,
} from "../../components/PermissionFormModal";
import {
  useApplyPermission,
  useDeletePermission,
  ApplyPermissionPayload,
} from "../../queries/mutations/usePermissionMutations";
import useResourceQuery, {
  permissionListPath,
} from "../../queries/useResourceQuery";

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

const TYPE_DISPLAY_NAMES: Record<string, string> = {
  FEATURE_VIEW: "Feature View",
  ON_DEMAND_FEATURE_VIEW: "On-Demand Feature View",
  BATCH_FEATURE_VIEW: "Batch Feature View",
  STREAM_FEATURE_VIEW: "Stream Feature View",
  ENTITY: "Entity",
  FEATURE_SERVICE: "Feature Service",
  DATA_SOURCE: "Data Source",
  VALIDATION_REFERENCE: "Validation Reference",
  SAVED_DATASET: "Saved Dataset",
  PERMISSION: "Permission",
  PROJECT: "Project",
  LABEL_VIEW: "Label View",
};

const resolveType = (t: string | number): string => {
  if (typeof t === "string") return t;
  const numericMap: Record<number, string> = {
    0: "FEATURE_VIEW",
    1: "ON_DEMAND_FEATURE_VIEW",
    2: "BATCH_FEATURE_VIEW",
    3: "STREAM_FEATURE_VIEW",
    4: "ENTITY",
    5: "FEATURE_SERVICE",
    6: "DATA_SOURCE",
    7: "VALIDATION_REFERENCE",
    8: "SAVED_DATASET",
    9: "PERMISSION",
    10: "PROJECT",
    11: "LABEL_VIEW",
  };
  return numericMap[t] || `Type ${t}`;
};

const resolveAction = (a: string | number): string => {
  if (typeof a === "string") return a;
  return ACTION_NAMES[a] || `Action ${a}`;
};

const getActionColor = (action: string) => {
  if (action.startsWith("READ")) return "success";
  if (action.startsWith("WRITE")) return "warning";
  if (action === "CREATE") return "primary";
  if (action === "UPDATE") return "accent";
  if (action === "DELETE") return "danger";
  if (action === "DESCRIBE") return "hollow";
  return "default";
};

const getPolicyDescription = (policy: any): string => {
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
    return parts.join(" | ");
  }
  return "Allow All";
};

const getPolicyType = (
  policy: any,
): "role_based" | "group_based" | "namespace_based" | "combined" => {
  if (!policy) return "role_based";
  if (policy.roleBasedPolicy) return "role_based";
  if (policy.groupBasedPolicy) return "group_based";
  if (policy.namespaceBasedPolicy) return "namespace_based";
  if (policy.combinedGroupNamespacePolicy) return "combined";
  return "role_based";
};

const permissionToFormData = (permission: any): PermissionFormData => {
  const spec = permission.spec || permission;
  const policy = spec.policy;

  const rawTypes: string[] = (spec.types || []).map(resolveType);
  const rawActions: string[] = (spec.actions || []).map(resolveAction);

  return {
    name: spec.name || "",
    types: Array.from(new Set(rawTypes)),
    namePatterns: spec.namePatterns || spec.name_patterns || [],
    actions: Array.from(new Set(rawActions)),
    policyType: getPolicyType(policy),
    roles: policy?.roleBasedPolicy?.roles || [],
    groups:
      policy?.groupBasedPolicy?.groups ||
      policy?.combinedGroupNamespacePolicy?.groups ||
      [],
    namespaces:
      policy?.namespaceBasedPolicy?.namespaces ||
      policy?.combinedGroupNamespacePolicy?.namespaces ||
      [],
    tags: Object.entries(spec.tags || {}).map(([key, value]) => ({
      key,
      value: value as string,
    })),
    requiredTags: Object.entries(
      spec.requiredTags || spec.required_tags || {},
    ).map(([key, value]) => ({
      key,
      value: value as string,
    })),
  };
};

const formDataToPayload = (
  formData: PermissionFormData,
  project: string,
): ApplyPermissionPayload => {
  const policy: ApplyPermissionPayload["policy"] = {};

  if (formData.policyType === "role_based") {
    policy.role_based_policy = {
      roles: formData.roles.filter((r) => r.trim()),
    };
  } else if (formData.policyType === "group_based") {
    policy.group_based_policy = {
      groups: formData.groups.filter((g) => g.trim()),
    };
  } else if (formData.policyType === "namespace_based") {
    policy.namespace_based_policy = {
      namespaces: formData.namespaces.filter((n) => n.trim()),
    };
  } else if (formData.policyType === "combined") {
    policy.combined_group_namespace_policy = {
      groups: formData.groups.filter((g) => g.trim()),
      namespaces: formData.namespaces.filter((n) => n.trim()),
    };
  }

  return {
    name: formData.name,
    project,
    types: formData.types,
    name_patterns: formData.namePatterns.filter((p) => p.trim()),
    actions: formData.actions,
    policy,
    tags: Object.fromEntries(
      formData.tags.filter((t) => t.key.trim()).map((t) => [t.key, t.value]),
    ),
    required_tags: Object.fromEntries(
      formData.requiredTags
        .filter((t) => t.key.trim())
        .map((t) => [t.key, t.value]),
    ),
  };
};

const useLoadPermissions = () => {
  const { projectName } = useParams();
  return useResourceQuery<any[]>({
    resourceType: "permissions-list",
    project: projectName,
    restPath: permissionListPath(projectName),
    restSelect: (d) => d.permissions,
  });
};

const PermissionsIndex = () => {
  const { projectName } = useParams();
  const { isLoading, isSuccess, isError, isPermissionDenied, data } =
    useLoadPermissions();

  const [isModalOpen, setIsModalOpen] = useState(false);
  const [editingPermission, setEditingPermission] = useState<any | null>(null);
  const [deleteTarget, setDeleteTarget] = useState<any | null>(null);
  const [successMessage, setSuccessMessage] = useState<string | null>(null);
  const [errorMessage, setErrorMessage] = useState<string | null>(null);
  const [searchString, setSearchString] = useState("");
  const [actionFilter, setActionFilter] = useState("");

  const applyPermission = useApplyPermission();
  const deletePermissionMutation = useDeletePermission();

  const permissions = useMemo(() => {
    if (!data) return [];
    let filtered = data;

    if (searchString.trim()) {
      const lower = searchString.toLowerCase();
      filtered = filtered.filter((p) =>
        (p.spec?.name || "").toLowerCase().includes(lower),
      );
    }

    if (actionFilter) {
      filtered = filtered.filter((p) => {
        const actions = (p.spec?.actions || []).map(resolveAction);
        return actions.includes(actionFilter);
      });
    }

    return filtered;
  }, [data, searchString, actionFilter]);

  const handleCreate = () => {
    setEditingPermission(null);
    setIsModalOpen(true);
  };

  const handleEdit = (permission: any) => {
    setEditingPermission(permission);
    setIsModalOpen(true);
  };

  const handleDelete = (permission: any) => {
    setDeleteTarget(permission);
  };

  const confirmDelete = () => {
    if (!deleteTarget) return;
    const name = deleteTarget.spec?.name || deleteTarget.name;
    deletePermissionMutation.mutate(
      { name, project: projectName || "" },
      {
        onSuccess: () => {
          setDeleteTarget(null);
          setErrorMessage(null);
          setSuccessMessage(`Permission "${name}" deleted successfully.`);
          setTimeout(() => setSuccessMessage(null), 5000);
        },
        onError: (err: unknown) => {
          setDeleteTarget(null);
          const message =
            err instanceof Error
              ? err.message
              : "An unexpected error occurred.";
          setErrorMessage(message);
          setTimeout(() => setErrorMessage(null), 5000);
        },
      },
    );
  };

  const handleFormSubmit = (formData: PermissionFormData) => {
    const payload = formDataToPayload(formData, projectName || "");
    applyPermission.mutate(payload, {
      onSuccess: () => {
        setIsModalOpen(false);
        setEditingPermission(null);
        setErrorMessage(null);
        const verb = editingPermission ? "updated" : "created";
        setSuccessMessage(
          `Permission "${formData.name}" ${verb} successfully.`,
        );
        setTimeout(() => setSuccessMessage(null), 5000);
      },
      onError: (err: unknown) => {
        const message =
          err instanceof Error ? err.message : "An unexpected error occurred.";
        setErrorMessage(message);
      },
    });
  };

  const columns = [
    {
      field: "spec.name",
      name: "Name",
      sortable: true,
      render: (_: string, item: any) => (
        <strong>{item.spec?.name || "—"}</strong>
      ),
    },
    {
      field: "spec.types",
      name: "Resource Types",
      render: (_: any, item: any) => {
        const rawTypes: string[] = (item.spec?.types || []).map(resolveType);
        const types = Array.from(new Set(rawTypes));
        if (types.length === 0) return <EuiText size="xs">All</EuiText>;
        const display = (t: string) => TYPE_DISPLAY_NAMES[t] || t;
        if (types.length > 3) {
          return (
            <EuiToolTip content={types.map(display).join(", ")}>
              <EuiText size="xs">
                {types.slice(0, 3).map(display).join(", ")} +{types.length - 3}{" "}
                more
              </EuiText>
            </EuiToolTip>
          );
        }
        return <EuiText size="xs">{types.map(display).join(", ")}</EuiText>;
      },
    },
    {
      field: "spec.actions",
      name: "Actions",
      render: (_: any, item: any) => {
        const rawActions: string[] = (item.spec?.actions || []).map(
          resolveAction,
        );
        const actions = Array.from(new Set(rawActions));
        return (
          <EuiFlexGroup wrap responsive={false} gutterSize="xs">
            {actions.map((action: string, i: number) => (
              <EuiFlexItem grow={false} key={i}>
                <EuiBadge color={getActionColor(action)}>{action}</EuiBadge>
              </EuiFlexItem>
            ))}
          </EuiFlexGroup>
        );
      },
    },
    {
      field: "spec.policy",
      name: "Policy",
      render: (_: any, item: any) => {
        const desc = getPolicyDescription(item.spec?.policy);
        return (
          <EuiToolTip content={desc}>
            <EuiText size="xs">
              {desc.length > 40 ? desc.substring(0, 40) + "..." : desc}
            </EuiText>
          </EuiToolTip>
        );
      },
    },
    {
      name: "Actions",
      width: "100px",
      render: (item: any) => (
        <EuiFlexGroup gutterSize="xs" responsive={false}>
          <EuiFlexItem grow={false}>
            <EuiToolTip content="Edit">
              <EuiButtonIcon
                iconType="pencil"
                aria-label="Edit permission"
                onClick={() => handleEdit(item)}
                color="primary"
              />
            </EuiToolTip>
          </EuiFlexItem>
          <EuiFlexItem grow={false}>
            <EuiToolTip content="Delete">
              <EuiButtonIcon
                iconType="trash"
                aria-label="Delete permission"
                onClick={() => handleDelete(item)}
                color="danger"
              />
            </EuiToolTip>
          </EuiFlexItem>
        </EuiFlexGroup>
      ),
    },
  ];

  const hasPermissions = isSuccess && data && data.length > 0;
  const isEmpty = isSuccess && (!data || data.length === 0);

  return (
    <EuiPageTemplate panelled>
      <EuiPageTemplate.Header
        restrictWidth
        iconType={PermissionsIcon}
        pageTitle="Permissions"
        description="Create and manage access control rules for Feast resources"
        rightSideItems={[
          <EuiButton fill iconType="plus" onClick={handleCreate} key="create">
            Create Permission
          </EuiButton>,
        ]}
      />
      <EuiPageTemplate.Section>
        {successMessage && (
          <>
            <EuiCallOut
              title={successMessage}
              color="success"
              iconType="check"
              size="s"
            />
            <EuiSpacer size="m" />
          </>
        )}
        {errorMessage && !isModalOpen && (
          <>
            <EuiCallOut
              title={errorMessage}
              color="danger"
              iconType="alert"
              size="s"
            />
            <EuiSpacer size="m" />
          </>
        )}

        {isLoading && (
          <p>
            <EuiLoadingSpinner size="m" /> Loading
          </p>
        )}
        {isPermissionDenied && (
          <EuiCallOut title="Permission denied" color="warning" iconType="lock">
            <p>You do not have permission to view permissions.</p>
          </EuiCallOut>
        )}
        {isError && !isPermissionDenied && <p>Error loading permissions.</p>}

        {isEmpty && (
          <EuiEmptyPrompt
            iconType="lock"
            title={<h2>No permissions yet</h2>}
            body={
              <p>
                Permissions let you control who can perform specific actions on
                your Feast resources. Create your first permission to get
                started.
              </p>
            }
            actions={
              <EuiButton fill iconType="plus" onClick={handleCreate}>
                Create Permission
              </EuiButton>
            }
          />
        )}

        {hasPermissions && (
          <>
            <EuiFlexGroup gutterSize="m">
              <EuiFlexItem grow={3}>
                <EuiTitle size="xs">
                  <h2>Search</h2>
                </EuiTitle>
                <EuiFieldSearch
                  value={searchString}
                  fullWidth
                  placeholder="Filter by name..."
                  onChange={(e) => setSearchString(e.target.value)}
                />
              </EuiFlexItem>
              <EuiFlexItem grow={1}>
                <EuiFormRow label="Filter by action">
                  <EuiSelect
                    options={[
                      { value: "", text: "All Actions" },
                      ...ACTION_NAMES.map((a) => ({ value: a, text: a })),
                    ]}
                    value={actionFilter}
                    onChange={(e) => setActionFilter(e.target.value)}
                  />
                </EuiFormRow>
              </EuiFlexItem>
            </EuiFlexGroup>
            <EuiSpacer size="m" />
            <EuiBasicTable
              items={permissions}
              columns={columns}
              rowHeader="spec.name"
            />
          </>
        )}
      </EuiPageTemplate.Section>

      {isModalOpen && (
        <PermissionFormModal
          onClose={() => {
            setIsModalOpen(false);
            setEditingPermission(null);
            setErrorMessage(null);
          }}
          onSubmit={handleFormSubmit}
          isEdit={!!editingPermission}
          initialData={
            editingPermission
              ? permissionToFormData(editingPermission)
              : undefined
          }
          isSubmitting={applyPermission.isLoading}
          submitError={errorMessage}
        />
      )}

      {deleteTarget && (
        <EuiConfirmModal
          title="Delete permission"
          onCancel={() => setDeleteTarget(null)}
          onConfirm={confirmDelete}
          cancelButtonText="Cancel"
          confirmButtonText="Delete"
          buttonColor="danger"
          isLoading={deletePermissionMutation.isLoading}
        >
          <p>
            Are you sure you want to delete the permission{" "}
            <strong>
              &quot;{deleteTarget.spec?.name || deleteTarget.name}&quot;
            </strong>
            ? This action cannot be undone.
          </p>
        </EuiConfirmModal>
      )}
    </EuiPageTemplate>
  );
};

export default PermissionsIndex;
