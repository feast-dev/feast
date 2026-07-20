import React, { useState } from "react";
import {
  EuiFormRow,
  EuiFieldText,
  EuiSpacer,
  EuiCallOut,
  EuiCheckboxGroup,
  EuiRadioGroup,
  EuiTitle,
  EuiHorizontalRule,
  EuiText,
  EuiFlexGroup,
  EuiFlexItem,
  EuiButtonEmpty,
  EuiFieldText as EuiField,
  EuiButtonIcon,
  EuiToolTip,
  EuiPanel,
} from "@elastic/eui";
import FormModal from "./forms/FormModal";
import TagsEditor, { TagEntry } from "./forms/TagsEditor";

const RESOURCE_TYPES = [
  { id: "FEATURE_VIEW", label: "Feature View" },
  { id: "ON_DEMAND_FEATURE_VIEW", label: "On-Demand Feature View" },
  { id: "BATCH_FEATURE_VIEW", label: "Batch Feature View" },
  { id: "STREAM_FEATURE_VIEW", label: "Stream Feature View" },
  { id: "ENTITY", label: "Entity" },
  { id: "FEATURE_SERVICE", label: "Feature Service" },
  { id: "DATA_SOURCE", label: "Data Source" },
  { id: "VALIDATION_REFERENCE", label: "Validation Reference" },
  { id: "SAVED_DATASET", label: "Saved Dataset" },
  { id: "PERMISSION", label: "Permission" },
  { id: "PROJECT", label: "Project" },
  { id: "LABEL_VIEW", label: "Label View" },
];

const ACTIONS = [
  { id: "CREATE", label: "Create" },
  { id: "DESCRIBE", label: "Describe" },
  { id: "UPDATE", label: "Update" },
  { id: "DELETE", label: "Delete" },
  { id: "READ_ONLINE", label: "Read Online" },
  { id: "READ_OFFLINE", label: "Read Offline" },
  { id: "WRITE_ONLINE", label: "Write Online" },
  { id: "WRITE_OFFLINE", label: "Write Offline" },
];

const ACTION_PRESETS = [
  {
    id: "all",
    label: "All Actions",
    actions: ACTIONS.map((a) => a.id),
  },
  {
    id: "crud",
    label: "CRUD Only",
    actions: ["CREATE", "DESCRIBE", "UPDATE", "DELETE"],
  },
  {
    id: "read",
    label: "Read Only",
    actions: ["DESCRIBE", "READ_ONLINE", "READ_OFFLINE"],
  },
  {
    id: "write",
    label: "Write Only",
    actions: ["WRITE_ONLINE", "WRITE_OFFLINE"],
  },
];

type PolicyType = "role_based" | "group_based" | "namespace_based" | "combined";

const POLICY_TYPE_OPTIONS = [
  { id: "role_based", label: "Role-based" },
  { id: "group_based", label: "Group-based" },
  { id: "namespace_based", label: "Namespace-based" },
  { id: "combined", label: "Combined (Groups + Namespaces)" },
];

interface PermissionFormData {
  name: string;
  types: string[];
  namePatterns: string[];
  actions: string[];
  policyType: PolicyType;
  roles: string[];
  groups: string[];
  namespaces: string[];
  tags: TagEntry[];
  requiredTags: TagEntry[];
}

interface PermissionFormModalProps {
  onClose: () => void;
  onSubmit: (data: PermissionFormData) => void;
  initialData?: PermissionFormData;
  isEdit?: boolean;
  isSubmitting?: boolean;
  submitError?: string | null;
}

const EMPTY_FORM: PermissionFormData = {
  name: "",
  types: [],
  namePatterns: [],
  actions: [],
  policyType: "role_based",
  roles: [],
  groups: [],
  namespaces: [],
  tags: [],
  requiredTags: [],
};

const StringListEditor: React.FC<{
  items: string[];
  onChange: (items: string[]) => void;
  placeholder: string;
  addLabel: string;
}> = ({ items, onChange, placeholder, addLabel }) => {
  return (
    <>
      {items.map((item, index) => (
        <EuiFlexGroup
          key={index}
          gutterSize="s"
          alignItems="center"
          style={{ marginTop: 4 }}
        >
          <EuiFlexItem>
            <EuiField
              placeholder={placeholder}
              value={item}
              onChange={(e) => {
                const updated = [...items];
                updated[index] = e.target.value;
                onChange(updated);
              }}
              compressed
            />
          </EuiFlexItem>
          <EuiFlexItem grow={false}>
            <EuiButtonIcon
              iconType="trash"
              color="danger"
              aria-label={`Remove ${placeholder.toLowerCase()}`}
              onClick={() => onChange(items.filter((_, i) => i !== index))}
            />
          </EuiFlexItem>
        </EuiFlexGroup>
      ))}
      <EuiSpacer size="xs" />
      <EuiButtonEmpty
        size="s"
        iconType="plus"
        onClick={() => onChange([...items, ""])}
        flush="left"
      >
        {addLabel}
      </EuiButtonEmpty>
    </>
  );
};

const PermissionFormModal: React.FC<PermissionFormModalProps> = ({
  onClose,
  onSubmit,
  initialData,
  isEdit = false,
  isSubmitting = false,
  submitError,
}) => {
  const [formData, setFormData] = useState<PermissionFormData>(
    initialData || EMPTY_FORM,
  );
  const [errors, setErrors] = useState<Record<string, string>>({});

  const validate = (): boolean => {
    const newErrors: Record<string, string> = {};

    if (!formData.name.trim()) {
      newErrors.name = "Permission name is required.";
    } else if (!/^[a-zA-Z_][a-zA-Z0-9_-]*$/.test(formData.name)) {
      newErrors.name =
        "Must start with a letter or underscore, and contain only letters, numbers, underscores, and hyphens.";
    }

    if (formData.types.length === 0) {
      newErrors.types = "Select at least one resource type.";
    }

    if (formData.actions.length === 0) {
      newErrors.actions = "Select at least one action.";
    }

    if (formData.policyType === "role_based") {
      const nonEmpty = formData.roles.filter((r) => r.trim());
      if (nonEmpty.length === 0) {
        newErrors.policy = "Add at least one role.";
      }
    } else if (formData.policyType === "group_based") {
      const nonEmpty = formData.groups.filter((g) => g.trim());
      if (nonEmpty.length === 0) {
        newErrors.policy = "Add at least one group.";
      }
    } else if (formData.policyType === "namespace_based") {
      const nonEmpty = formData.namespaces.filter((n) => n.trim());
      if (nonEmpty.length === 0) {
        newErrors.policy = "Add at least one namespace.";
      }
    } else if (formData.policyType === "combined") {
      const hasGroups = formData.groups.filter((g) => g.trim()).length > 0;
      const hasNamespaces =
        formData.namespaces.filter((n) => n.trim()).length > 0;
      if (!hasGroups && !hasNamespaces) {
        newErrors.policy = "Add at least one group or namespace.";
      }
    }

    setErrors(newErrors);
    return Object.keys(newErrors).length === 0;
  };

  const handleSubmit = () => {
    if (validate()) {
      onSubmit(formData);
    }
  };

  const selectedTypesMap: Record<string, boolean> = {};
  formData.types.forEach((t) => {
    selectedTypesMap[t] = true;
  });

  const selectedActionsMap: Record<string, boolean> = {};
  formData.actions.forEach((a) => {
    selectedActionsMap[a] = true;
  });

  const handleTypeToggle = (id: string) => {
    setFormData((prev) => ({
      ...prev,
      types: prev.types.includes(id)
        ? prev.types.filter((t) => t !== id)
        : [...prev.types, id],
    }));
  };

  const handleActionToggle = (id: string) => {
    setFormData((prev) => ({
      ...prev,
      actions: prev.actions.includes(id)
        ? prev.actions.filter((a) => a !== id)
        : [...prev.actions, id],
    }));
  };

  const applyActionPreset = (presetActions: string[]) => {
    setFormData((prev) => ({ ...prev, actions: [...presetActions] }));
  };

  const selectAllTypes = () => {
    setFormData((prev) => ({
      ...prev,
      types: RESOURCE_TYPES.map((t) => t.id),
    }));
  };

  const clearAllTypes = () => {
    setFormData((prev) => ({ ...prev, types: [] }));
  };

  return (
    <FormModal
      title={isEdit ? "Edit Permission" : "Create Permission"}
      submitLabel={isEdit ? "Update" : "Create Permission"}
      onClose={onClose}
      onSubmit={handleSubmit}
      isSubmitting={isSubmitting}
      width={700}
    >
      {submitError && (
        <>
          <EuiCallOut title={submitError} color="danger" size="s" />
          <EuiSpacer size="m" />
        </>
      )}

      {/* Section 1: Identity */}
      <EuiTitle size="xxs">
        <h3>Identity</h3>
      </EuiTitle>
      <EuiSpacer size="s" />

      <EuiFormRow
        label="Permission Name"
        isInvalid={!!errors.name}
        error={errors.name}
        helpText="A unique name to identify this permission rule."
      >
        <EuiFieldText
          value={formData.name}
          onChange={(e) =>
            setFormData((prev) => ({ ...prev, name: e.target.value }))
          }
          isInvalid={!!errors.name}
          disabled={isEdit}
          placeholder="e.g. data-team-read-access"
        />
      </EuiFormRow>

      <EuiHorizontalRule margin="m" />

      {/* Section 2: Resource Scope */}
      <EuiTitle size="xxs">
        <h3>Resource Scope</h3>
      </EuiTitle>
      <EuiText size="xs" color="subdued">
        <p>Define which resources this permission applies to.</p>
      </EuiText>
      <EuiSpacer size="s" />

      <EuiFormRow
        label="Resource Types"
        isInvalid={!!errors.types}
        error={errors.types}
      >
        <>
          <EuiFlexGroup gutterSize="s" style={{ marginBottom: 8 }}>
            <EuiFlexItem grow={false}>
              <EuiButtonEmpty size="xs" onClick={selectAllTypes}>
                Select All
              </EuiButtonEmpty>
            </EuiFlexItem>
            <EuiFlexItem grow={false}>
              <EuiButtonEmpty size="xs" onClick={clearAllTypes}>
                Clear All
              </EuiButtonEmpty>
            </EuiFlexItem>
          </EuiFlexGroup>
          <EuiPanel paddingSize="s" hasBorder>
            <EuiCheckboxGroup
              options={RESOURCE_TYPES}
              idToSelectedMap={selectedTypesMap}
              onChange={handleTypeToggle}
              compressed
            />
          </EuiPanel>
        </>
      </EuiFormRow>

      <EuiSpacer size="m" />

      <EuiFormRow
        label={
          <EuiToolTip content="Regex patterns to match specific resource names. Leave empty to match all resources of the selected types.">
            <span>
              Name Patterns{" "}
              <EuiText size="xs" color="subdued" component="span">
                (optional)
              </EuiText>
            </span>
          </EuiToolTip>
        }
      >
        <StringListEditor
          items={formData.namePatterns}
          onChange={(namePatterns) =>
            setFormData((prev) => ({ ...prev, namePatterns }))
          }
          placeholder="e.g. driver_.*"
          addLabel="Add name pattern"
        />
      </EuiFormRow>

      <EuiSpacer size="s" />
      <EuiFormRow
        label={
          <EuiToolTip content="Tags that a resource must have for this permission to apply.">
            <span>
              Required Tags{" "}
              <EuiText size="xs" color="subdued" component="span">
                (optional)
              </EuiText>
            </span>
          </EuiToolTip>
        }
      >
        <TagsEditor
          tags={formData.requiredTags}
          onChange={(requiredTags) =>
            setFormData((prev) => ({ ...prev, requiredTags }))
          }
        />
      </EuiFormRow>

      <EuiHorizontalRule margin="m" />

      {/* Section 3: Actions */}
      <EuiTitle size="xxs">
        <h3>Allowed Actions</h3>
      </EuiTitle>
      <EuiText size="xs" color="subdued">
        <p>Which operations are permitted on matching resources.</p>
      </EuiText>
      <EuiSpacer size="s" />

      <EuiFormRow isInvalid={!!errors.actions} error={errors.actions}>
        <>
          <EuiFlexGroup gutterSize="s" style={{ marginBottom: 8 }} wrap>
            {ACTION_PRESETS.map((preset) => (
              <EuiFlexItem grow={false} key={preset.id}>
                <EuiButtonEmpty
                  size="xs"
                  onClick={() => applyActionPreset(preset.actions)}
                  color={
                    JSON.stringify([...formData.actions].sort()) ===
                    JSON.stringify([...preset.actions].sort())
                      ? "primary"
                      : "text"
                  }
                >
                  {preset.label}
                </EuiButtonEmpty>
              </EuiFlexItem>
            ))}
          </EuiFlexGroup>
          <EuiPanel paddingSize="s" hasBorder>
            <EuiCheckboxGroup
              options={ACTIONS}
              idToSelectedMap={selectedActionsMap}
              onChange={handleActionToggle}
              compressed
            />
          </EuiPanel>
        </>
      </EuiFormRow>

      <EuiHorizontalRule margin="m" />

      {/* Section 4: Policy */}
      <EuiTitle size="xxs">
        <h3>Access Policy</h3>
      </EuiTitle>
      <EuiText size="xs" color="subdued">
        <p>
          Define who is authorized. A user must match at least one entry in the
          policy to be granted access.
        </p>
      </EuiText>
      <EuiSpacer size="s" />

      <EuiFormRow label="Policy Type">
        <EuiRadioGroup
          options={POLICY_TYPE_OPTIONS}
          idSelected={formData.policyType}
          onChange={(id) =>
            setFormData((prev) => ({
              ...prev,
              policyType: id as PolicyType,
            }))
          }
          compressed
        />
      </EuiFormRow>

      <EuiSpacer size="s" />

      {errors.policy && (
        <>
          <EuiCallOut title={errors.policy} color="danger" size="s" />
          <EuiSpacer size="s" />
        </>
      )}

      {(formData.policyType === "role_based" ||
        formData.policyType === "combined") && (
        <EuiFormRow
          label={formData.policyType === "combined" ? "Groups" : undefined}
        >
          <>
            {formData.policyType === "role_based" && (
              <EuiText size="xs" color="subdued">
                <p>User must have at least one of these roles.</p>
              </EuiText>
            )}
            {formData.policyType === "role_based" && (
              <StringListEditor
                items={formData.roles}
                onChange={(roles) =>
                  setFormData((prev) => ({ ...prev, roles }))
                }
                placeholder="e.g. admin, data-engineer"
                addLabel="Add role"
              />
            )}
          </>
        </EuiFormRow>
      )}

      {(formData.policyType === "group_based" ||
        formData.policyType === "combined") && (
        <EuiFormRow
          label={formData.policyType === "combined" ? "Groups" : undefined}
        >
          <>
            {formData.policyType === "group_based" && (
              <EuiText size="xs" color="subdued">
                <p>User must belong to at least one of these groups.</p>
              </EuiText>
            )}
            <StringListEditor
              items={formData.groups}
              onChange={(groups) =>
                setFormData((prev) => ({ ...prev, groups }))
              }
              placeholder="e.g. ml-team, platform-team"
              addLabel="Add group"
            />
          </>
        </EuiFormRow>
      )}

      {(formData.policyType === "namespace_based" ||
        formData.policyType === "combined") && (
        <EuiFormRow
          label={formData.policyType === "combined" ? "Namespaces" : undefined}
        >
          <>
            {formData.policyType === "namespace_based" && (
              <EuiText size="xs" color="subdued">
                <p>User must be in at least one of these namespaces.</p>
              </EuiText>
            )}
            <StringListEditor
              items={formData.namespaces}
              onChange={(namespaces) =>
                setFormData((prev) => ({ ...prev, namespaces }))
              }
              placeholder="e.g. production, staging"
              addLabel="Add namespace"
            />
          </>
        </EuiFormRow>
      )}

      {formData.policyType === "combined" && (
        <>
          <EuiSpacer size="s" />
          <EuiText size="xs" color="subdued">
            <p>
              User must match at least one group <strong>or</strong> one
              namespace.
            </p>
          </EuiText>
        </>
      )}

      <EuiHorizontalRule margin="m" />

      {/* Section 5: Tags */}
      <EuiTitle size="xxs">
        <h3>
          Metadata Tags{" "}
          <EuiText size="xs" color="subdued" component="span">
            (optional)
          </EuiText>
        </h3>
      </EuiTitle>
      <EuiSpacer size="s" />
      <TagsEditor
        tags={formData.tags}
        onChange={(tags) => setFormData((prev) => ({ ...prev, tags }))}
      />
    </FormModal>
  );
};

export default PermissionFormModal;
export type { PermissionFormData };
