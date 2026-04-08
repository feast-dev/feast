import React, { useState, useEffect, useContext } from "react";
import {
  EuiFormRow,
  EuiFieldText,
  EuiFieldNumber,
  EuiSelect,
  EuiSwitch,
  EuiComboBox,
  EuiComboBoxOptionOption,
  EuiSpacer,
} from "@elastic/eui";
import { useParams } from "react-router-dom";
import FormModal from "./forms/FormModal";
import TagsEditor, { TagEntry } from "./forms/TagsEditor";
import NameDescriptionOwnerFields from "./forms/NameDescriptionOwnerFields";
import FeatureFieldEditor, {
  FeatureFieldEntry,
} from "./forms/FeatureFieldEditor";
import RegistryPathContext from "../contexts/RegistryPathContext";
import useLoadRegistry from "../queries/useLoadRegistry";

const TTL_UNIT_OPTIONS = [
  { value: "seconds", text: "Seconds" },
  { value: "minutes", text: "Minutes" },
  { value: "hours", text: "Hours" },
  { value: "days", text: "Days" },
];

interface FeatureViewFormData {
  name: string;
  description: string;
  owner: string;
  entities: string[];
  features: FeatureFieldEntry[];
  batchSource: string;
  ttlValue: number;
  ttlUnit: string;
  online: boolean;
  tags: TagEntry[];
}

interface FeatureViewFormModalProps {
  onClose: () => void;
  onSubmit: (data: FeatureViewFormData) => void;
  initialData?: FeatureViewFormData;
  isEdit?: boolean;
}

const EMPTY_FORM: FeatureViewFormData = {
  name: "",
  description: "",
  owner: "",
  entities: [],
  features: [],
  batchSource: "",
  ttlValue: 0,
  ttlUnit: "seconds",
  online: true,
  tags: [],
};

const FeatureViewFormModal: React.FC<FeatureViewFormModalProps> = ({
  onClose,
  onSubmit,
  initialData,
  isEdit = false,
}) => {
  const [formData, setFormData] = useState<FeatureViewFormData>(
    initialData || EMPTY_FORM,
  );
  const [errors, setErrors] = useState<Record<string, string>>({});
  const [submitted, setSubmitted] = useState(false);

  const registryUrl = useContext(RegistryPathContext);
  const { projectName } = useParams();
  const registryQuery = useLoadRegistry(registryUrl, projectName);

  const entityOptions: EuiComboBoxOptionOption<string>[] =
    registryQuery.data?.objects?.entities?.map((e: any) => ({
      label: e?.spec?.name || "",
    })) || [];

  const dataSourceOptions =
    registryQuery.data?.objects?.dataSources?.map((ds: any) => ({
      value: ds?.name || "",
      text: ds?.name || "",
    })) || [];

  useEffect(() => {
    if (initialData) {
      setFormData(initialData);
    }
  }, [initialData]);

  const validate = (): boolean => {
    const newErrors: Record<string, string> = {};

    if (!formData.name.trim()) {
      newErrors.name = "Feature view name is required.";
    } else if (!/^[a-zA-Z_][a-zA-Z0-9_]*$/.test(formData.name)) {
      newErrors.name =
        "Must start with a letter or underscore, and contain only letters, numbers, and underscores.";
    }

    if (formData.features.length === 0) {
      newErrors.features = "At least one feature is required.";
    } else {
      const hasEmptyName = formData.features.some((f) => !f.name.trim());
      if (hasEmptyName) {
        newErrors.features = "All features must have a name.";
      }
      const featureNames = formData.features.map((f) => f.name.trim());
      if (new Set(featureNames).size !== featureNames.length) {
        newErrors.features = "Feature names must be unique.";
      }
    }

    if (!formData.batchSource.trim()) {
      newErrors.batchSource = "A batch source is required.";
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

  const updateField = <K extends keyof FeatureViewFormData>(
    field: K,
    value: FeatureViewFormData[K],
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

  const selectedEntityOptions = formData.entities.map((e) => ({ label: e }));

  return (
    <FormModal
      title={isEdit ? "Edit Feature View" : "Create Feature View"}
      submitLabel={isEdit ? "Update Feature View" : "Create Feature View"}
      onClose={onClose}
      onSubmit={handleSubmit}
      width={750}
    >
      <NameDescriptionOwnerFields
        name={formData.name}
        description={formData.description}
        owner={formData.owner}
        onChangeName={(v) => updateField("name", v)}
        onChangeDescription={(v) => updateField("description", v)}
        onChangeOwner={(v) => updateField("owner", v)}
        nameDisabled={isEdit}
        nameError={errors.name}
        nameHelpText="A unique name for this feature view."
        namePlaceholder="e.g. customer_features"
        descriptionPlaceholder="Describe what this feature view provides..."
      />

      <EuiFormRow
        label="Entities"
        helpText="Entities that this feature view is associated with."
      >
        <EuiComboBox
          placeholder="Select entities"
          options={entityOptions}
          selectedOptions={selectedEntityOptions}
          onChange={(selected) =>
            updateField(
              "entities",
              selected.map((s) => s.label),
            )
          }
          isClearable
        />
      </EuiFormRow>

      <EuiFormRow
        label="Batch Source"
        isInvalid={!!errors.batchSource}
        error={errors.batchSource}
        helpText="The data source providing batch/offline features."
      >
        {dataSourceOptions.length > 0 ? (
          <EuiSelect
            options={[
              { value: "", text: "-- Select a data source --" },
              ...dataSourceOptions,
            ]}
            value={formData.batchSource}
            onChange={(e) => updateField("batchSource", e.target.value)}
            isInvalid={!!errors.batchSource}
          />
        ) : (
          <EuiFieldText
            value={formData.batchSource}
            onChange={(e) => updateField("batchSource", e.target.value)}
            isInvalid={!!errors.batchSource}
            placeholder="data_source_name"
          />
        )}
      </EuiFormRow>

      <EuiSpacer size="m" />

      <FeatureFieldEditor
        features={formData.features}
        onChange={(features) => updateField("features", features)}
        error={errors.features}
      />

      <EuiSpacer size="m" />

      <EuiFormRow
        label="TTL (Time to Live)"
        helpText="How long features remain valid after their event timestamp."
      >
        <div style={{ display: "flex", gap: 8 }}>
          <EuiFieldNumber
            value={formData.ttlValue}
            onChange={(e) =>
              updateField("ttlValue", parseInt(e.target.value) || 0)
            }
            min={0}
            style={{ width: 120 }}
          />
          <EuiSelect
            options={TTL_UNIT_OPTIONS}
            value={formData.ttlUnit}
            onChange={(e) => updateField("ttlUnit", e.target.value)}
            style={{ width: 140 }}
          />
        </div>
      </EuiFormRow>

      <EuiFormRow label="Online serving">
        <EuiSwitch
          label="Serve features online"
          checked={formData.online}
          onChange={(e) => updateField("online", e.target.checked)}
        />
      </EuiFormRow>

      <EuiSpacer size="m" />

      <TagsEditor
        tags={formData.tags}
        onChange={(tags) => updateField("tags", tags)}
        error={errors.tags}
      />
    </FormModal>
  );
};

export default FeatureViewFormModal;
export type { FeatureViewFormData };
