import React, { useState, useEffect } from "react";
import { EuiFormRow, EuiFieldText, EuiSpacer } from "@elastic/eui";
import { feast } from "../protos";
import FormModal from "./forms/FormModal";
import TagsEditor, { TagEntry } from "./forms/TagsEditor";
import NameDescriptionOwnerFields from "./forms/NameDescriptionOwnerFields";
import ValueTypeSelect from "./forms/ValueTypeSelect";

interface EntityFormData {
  name: string;
  description: string;
  joinKey: string;
  valueType: string;
  tags: TagEntry[];
}

interface EntityFormModalProps {
  onClose: () => void;
  onSubmit: (data: EntityFormData) => void;
  initialData?: EntityFormData;
  isEdit?: boolean;
}

const EMPTY_FORM: EntityFormData = {
  name: "",
  description: "",
  joinKey: "",
  valueType: String(feast.types.ValueType.Enum.STRING),
  tags: [],
};

const EntityFormModal: React.FC<EntityFormModalProps> = ({
  onClose,
  onSubmit,
  initialData,
  isEdit = false,
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

    if (!formData.joinKey.trim()) {
      newErrors.joinKey = "Join key is required.";
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

  return (
    <FormModal
      title={isEdit ? "Edit Entity" : "Create Entity"}
      submitLabel={isEdit ? "Update Entity" : "Create Entity"}
      onClose={onClose}
      onSubmit={handleSubmit}
    >
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

      <EuiFormRow
        label="Join Key"
        isInvalid={!!errors.joinKey}
        error={errors.joinKey}
        helpText="Column name used to join this entity in data sources."
      >
        <EuiFieldText
          value={formData.joinKey}
          onChange={(e) => updateField("joinKey", e.target.value)}
          isInvalid={!!errors.joinKey}
          placeholder="e.g. customer_id"
        />
      </EuiFormRow>

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
