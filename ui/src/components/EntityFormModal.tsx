import React, { useState, useEffect } from "react";
import {
  EuiSpacer,
  EuiFlexGroup,
  EuiFlexItem,
  EuiFieldText,
  EuiButtonEmpty,
  EuiButtonIcon,
  EuiText,
  EuiHorizontalRule,
  EuiCallOut,
} from "@elastic/eui";
import { feast } from "../protos";
import FormModal from "./forms/FormModal";
import TagsEditor, { TagEntry } from "./forms/TagsEditor";
import NameDescriptionOwnerFields from "./forms/NameDescriptionOwnerFields";
import ValueTypeSelect from "./forms/ValueTypeSelect";

interface EntityFormData {
  name: string;
  description: string;
  joinKeys: string[];
  valueType: string;
  tags: TagEntry[];
}

interface EntityFormModalProps {
  onClose: () => void;
  onSubmit: (data: EntityFormData) => void;
  initialData?: EntityFormData;
  isEdit?: boolean;
  isSubmitting?: boolean;
  submitError?: string | null;
}

const EMPTY_FORM: EntityFormData = {
  name: "",
  description: "",
  joinKeys: [""],
  valueType: String(feast.types.ValueType.Enum.STRING),
  tags: [],
};

const EntityFormModal: React.FC<EntityFormModalProps> = ({
  onClose,
  onSubmit,
  initialData,
  isEdit = false,
  isSubmitting = false,
  submitError,
}) => {
  const [formData, setFormData] = useState<EntityFormData>(
    initialData || EMPTY_FORM,
  );
  const [errors, setErrors] = useState<Record<string, string>>({});
  const [submitted, setSubmitted] = useState(false);

  useEffect(() => {
    if (initialData) {
      setFormData(initialData);
    }
  }, [initialData]);

  const validate = (): boolean => {
    const newErrors: Record<string, string> = {};

    if (!formData.name.trim()) {
      newErrors.name = "Entity name is required.";
    } else if (!/^[a-zA-Z_][a-zA-Z0-9_]*$/.test(formData.name)) {
      newErrors.name =
        "Must start with a letter or underscore, and contain only letters, numbers, and underscores.";
    }

    const nonEmptyKeys = formData.joinKeys.filter((k) => k.trim());
    if (nonEmptyKeys.length === 0) {
      newErrors.joinKeys = "At least one join key is required.";
    } else {
      if (new Set(nonEmptyKeys).size !== nonEmptyKeys.length) {
        newErrors.joinKeys = "Join keys must be unique.";
      }
      const invalidKey = nonEmptyKeys.find(
        (k) => !/^[a-zA-Z_][a-zA-Z0-9_]*$/.test(k),
      );
      if (invalidKey) {
        newErrors.joinKeys = `Invalid join key "${invalidKey}". Use only letters, numbers, and underscores.`;
      }
    }

    const tagKeys = formData.tags.map((t) => t.key).filter((k) => k.trim());
    if (new Set(tagKeys).size !== tagKeys.length) {
      newErrors.tags = "Tag keys must be unique.";
    }

    setErrors(newErrors);
    return Object.keys(newErrors).length === 0;
  };

  const handleSubmit = () => {
    setSubmitted(true);
    if (validate()) {
      const cleanedData = {
        ...formData,
        joinKeys: formData.joinKeys.filter((k) => k.trim()),
        tags: formData.tags.filter((t) => t.key.trim()),
      };
      onSubmit(cleanedData);
    }
  };

  const updateField = <K extends keyof EntityFormData>(
    field: K,
    value: EntityFormData[K],
  ) => {
    setFormData((prev) => ({ ...prev, [field]: value }));
    if (submitted) {
      setErrors((prev) => {
        const next = { ...prev };
        delete next[field];
        return next;
      });
    }
  };

  const addJoinKey = () => {
    updateField("joinKeys", [...formData.joinKeys, ""]);
  };

  const removeJoinKey = (index: number) => {
    if (formData.joinKeys.length <= 1) return;
    updateField(
      "joinKeys",
      formData.joinKeys.filter((_, i) => i !== index),
    );
  };

  const updateJoinKey = (index: number, value: string) => {
    const updated = [...formData.joinKeys];
    updated[index] = value;
    updateField("joinKeys", updated);
  };

  return (
    <FormModal
      title={isEdit ? "Edit Entity" : "Create Entity"}
      submitLabel={isEdit ? "Update Entity" : "Create Entity"}
      onClose={onClose}
      onSubmit={handleSubmit}
      isSubmitting={isSubmitting}
    >
      {submitError && (
        <>
          <EuiCallOut
            title={
              isEdit ? "Unable to update entity" : "Unable to create entity"
            }
            color="danger"
            iconType="alert"
            size="s"
          >
            <p>{submitError}</p>
          </EuiCallOut>
          <EuiSpacer size="m" />
        </>
      )}

      <NameDescriptionOwnerFields
        name={formData.name}
        description={formData.description}
        onChangeName={(v) => updateField("name", v)}
        onChangeDescription={(v) => updateField("description", v)}
        nameDisabled={isEdit}
        nameError={errors.name}
        nameHelpText="A unique identifier for this entity (e.g. customer_id)."
        namePlaceholder="e.g. customer_id"
        descriptionPlaceholder="Describe what this entity represents..."
      />

      <EuiSpacer size="m" />
      <EuiHorizontalRule margin="s" />

      <EuiFlexGroup alignItems="center" justifyContent="spaceBetween">
        <EuiFlexItem grow={false}>
          <EuiText size="s">
            <h4>Join Keys</h4>
          </EuiText>
        </EuiFlexItem>
        <EuiFlexItem grow={false}>
          <EuiButtonEmpty
            size="s"
            iconType="plus"
            onClick={addJoinKey}
            flush="right"
          >
            Add join key
          </EuiButtonEmpty>
        </EuiFlexItem>
      </EuiFlexGroup>

      <EuiText size="xs" color="subdued">
        Column name(s) used to join this entity in data sources.
      </EuiText>

      {errors.joinKeys && (
        <>
          <EuiSpacer size="s" />
          <EuiCallOut title={errors.joinKeys} color="danger" size="s" />
        </>
      )}

      {formData.joinKeys.map((key, index) => (
        <EuiFlexGroup
          key={index}
          gutterSize="s"
          alignItems="center"
          style={{ marginTop: 4 }}
        >
          <EuiFlexItem>
            <EuiFieldText
              value={key}
              onChange={(e) => updateJoinKey(index, e.target.value)}
              placeholder={
                index === 0 ? "e.g. customer_id" : "e.g. timestamp_field"
              }
              compressed
              isInvalid={!!errors.joinKeys && !key.trim()}
            />
          </EuiFlexItem>
          <EuiFlexItem grow={false}>
            <EuiButtonIcon
              iconType="trash"
              color="danger"
              aria-label="Remove join key"
              onClick={() => removeJoinKey(index)}
              disabled={formData.joinKeys.length <= 1}
            />
          </EuiFlexItem>
        </EuiFlexGroup>
      ))}

      <EuiSpacer size="m" />

      <ValueTypeSelect
        value={formData.valueType}
        onChange={(v) => updateField("valueType", v)}
        helpText="Data type of the join key."
      />

      <EuiSpacer size="m" />

      <TagsEditor
        tags={formData.tags}
        onChange={(tags) => updateField("tags", tags)}
        error={errors.tags}
      />
    </FormModal>
  );
};

export default EntityFormModal;
export type { EntityFormData, TagEntry };
