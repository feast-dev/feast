import React, { useState, useEffect, useMemo } from "react";
import {
  EuiSpacer,
  EuiFlexGroup,
  EuiFlexItem,
  EuiText,
  EuiHorizontalRule,
  EuiCallOut,
  EuiButtonEmpty,
  EuiButtonIcon,
  EuiFormRow,
  EuiComboBox,
  EuiComboBoxOptionOption,
} from "@elastic/eui";
import { useParams } from "react-router-dom";
import FormModal from "./forms/FormModal";
import TagsEditor, { TagEntry } from "./forms/TagsEditor";
import NameDescriptionOwnerFields from "./forms/NameDescriptionOwnerFields";
import useResourceQuery, {
  featureViewListPath,
  restFeatureViewsToMergedList,
} from "../queries/useResourceQuery";

interface FeatureViewProjectionEntry {
  featureViewName: string;
  featureNames: string[];
}

interface FeatureServiceFormData {
  name: string;
  description: string;
  owner: string;
  projections: FeatureViewProjectionEntry[];
  tags: TagEntry[];
}

interface FeatureServiceFormModalProps {
  onClose: () => void;
  onSubmit: (data: FeatureServiceFormData) => void;
  initialData?: FeatureServiceFormData;
  isEdit?: boolean;
  isSubmitting?: boolean;
  submitError?: string | null;
}

const EMPTY_PROJECTION: FeatureViewProjectionEntry = {
  featureViewName: "",
  featureNames: [],
};

const EMPTY_FORM: FeatureServiceFormData = {
  name: "",
  description: "",
  owner: "",
  projections: [{ ...EMPTY_PROJECTION }],
  tags: [],
};

const FeatureServiceFormModal: React.FC<FeatureServiceFormModalProps> = ({
  onClose,
  onSubmit,
  initialData,
  isEdit = false,
  isSubmitting = false,
  submitError,
}) => {
  const [formData, setFormData] = useState<FeatureServiceFormData>(
    initialData || EMPTY_FORM,
  );
  const [errors, setErrors] = useState<Record<string, string>>({});
  const [submitted, setSubmitted] = useState(false);

  const { projectName } = useParams();

  const featureViewsQuery = useResourceQuery<any[]>({
    resourceType: "feature-views-list-fs-form",
    project: projectName,
    restPath: featureViewListPath(projectName),
    restSelect: restFeatureViewsToMergedList,
  });

  const featureViews = useMemo(
    () => featureViewsQuery.data || [],
    [featureViewsQuery.data],
  );

  const featureViewOptions: EuiComboBoxOptionOption<string>[] = useMemo(
    () =>
      featureViews.map((fv) => ({
        label: fv.name,
        value: fv.name,
      })),
    [featureViews],
  );

  const featureNamesByView = useMemo(() => {
    const map: Record<string, string[]> = {};
    featureViews.forEach((fv) => {
      const features = fv.object?.spec?.features || [];
      map[fv.name] = features
        .map((feature: any) => feature?.name || "")
        .filter(Boolean);
    });
    return map;
  }, [featureViews]);

  useEffect(() => {
    if (initialData) {
      setFormData(initialData);
    }
  }, [initialData]);

  const validate = (): boolean => {
    const newErrors: Record<string, string> = {};

    if (!formData.name.trim()) {
      newErrors.name = "Feature service name is required.";
    } else if (!/^[a-zA-Z_][a-zA-Z0-9_]*$/.test(formData.name)) {
      newErrors.name =
        "Must start with a letter or underscore, and contain only letters, numbers, and underscores.";
    }

    const validProjections = formData.projections.filter((projection) =>
      projection.featureViewName.trim(),
    );
    if (validProjections.length === 0) {
      newErrors.projections = "Select at least one feature view.";
    }

    const viewNames = validProjections.map(
      (projection) => projection.featureViewName,
    );
    if (new Set(viewNames).size !== viewNames.length) {
      newErrors.projections = "Each feature view can only be selected once.";
    }

    const tagKeys = formData.tags
      .map((tag) => tag.key)
      .filter((key) => key.trim());
    if (new Set(tagKeys).size !== tagKeys.length) {
      newErrors.tags = "Tag keys must be unique.";
    }

    setErrors(newErrors);
    return Object.keys(newErrors).length === 0;
  };

  const handleSubmit = () => {
    setSubmitted(true);
    if (validate()) {
      onSubmit({
        ...formData,
        projections: formData.projections.filter((projection) =>
          projection.featureViewName.trim(),
        ),
        tags: formData.tags.filter((tag) => tag.key.trim()),
      });
    }
  };

  const updateField = <K extends keyof FeatureServiceFormData>(
    field: K,
    value: FeatureServiceFormData[K],
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

  const addProjection = () => {
    updateField("projections", [
      ...formData.projections,
      { ...EMPTY_PROJECTION },
    ]);
  };

  const removeProjection = (index: number) => {
    if (formData.projections.length <= 1) {
      return;
    }
    updateField(
      "projections",
      formData.projections.filter(
        (_, projectionIndex) => projectionIndex !== index,
      ),
    );
  };

  const updateProjection = (
    index: number,
    field: keyof FeatureViewProjectionEntry,
    value: string | string[],
  ) => {
    const updated = [...formData.projections];
    updated[index] = {
      ...updated[index],
      [field]: value,
      ...(field === "featureViewName" ? { featureNames: [] } : {}),
    };
    updateField("projections", updated);
  };

  return (
    <FormModal
      title={isEdit ? "Edit Feature Service" : "Create Feature Service"}
      submitLabel={isEdit ? "Update Feature Service" : "Create Feature Service"}
      onClose={onClose}
      onSubmit={handleSubmit}
      isSubmitting={isSubmitting}
    >
      {submitError && (
        <>
          <EuiCallOut
            title={
              isEdit
                ? "Unable to update feature service"
                : "Unable to create feature service"
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
        owner={formData.owner}
        onChangeName={(value) => updateField("name", value)}
        onChangeDescription={(value) => updateField("description", value)}
        onChangeOwner={(value) => updateField("owner", value)}
        nameDisabled={isEdit}
        nameError={errors.name}
        nameHelpText="A unique identifier for this feature service."
        namePlaceholder="e.g. customer_activity_v1"
        descriptionPlaceholder="Describe what this feature service is used for..."
      />

      <EuiSpacer size="m" />
      <EuiHorizontalRule margin="s" />

      <EuiFlexGroup alignItems="center" justifyContent="spaceBetween">
        <EuiFlexItem grow={false}>
          <EuiText size="s">
            <h4>Feature views</h4>
          </EuiText>
        </EuiFlexItem>
        <EuiFlexItem grow={false}>
          <EuiButtonEmpty
            size="s"
            iconType="plus"
            onClick={addProjection}
            flush="right"
          >
            Add feature view
          </EuiButtonEmpty>
        </EuiFlexItem>
      </EuiFlexGroup>

      <EuiText size="xs" color="subdued">
        Select one or more feature views to include in this service. Leave
        feature selection empty to include all features from a view.
      </EuiText>

      {errors.projections && (
        <>
          <EuiSpacer size="s" />
          <EuiCallOut title={errors.projections} color="danger" size="s" />
        </>
      )}

      {formData.projections.map((projection, index) => {
        const availableFeatures =
          featureNamesByView[projection.featureViewName] || [];
        const selectedFeatureOptions = projection.featureNames.map(
          (featureName) => ({
            label: featureName,
            value: featureName,
          }),
        );
        const featureOptions = availableFeatures.map((featureName) => ({
          label: featureName,
          value: featureName,
        }));

        return (
          <React.Fragment key={`projection-${index}`}>
            <EuiSpacer size="m" />
            <EuiFlexGroup alignItems="flexStart" gutterSize="s">
              <EuiFlexItem>
                <EuiFormRow label="Feature view">
                  <EuiComboBox
                    singleSelection={{ asPlainText: true }}
                    options={featureViewOptions}
                    selectedOptions={
                      projection.featureViewName
                        ? [
                            {
                              label: projection.featureViewName,
                              value: projection.featureViewName,
                            },
                          ]
                        : []
                    }
                    onChange={(selected) =>
                      updateProjection(
                        index,
                        "featureViewName",
                        selected.length > 0 ? selected[0].value || "" : "",
                      )
                    }
                    placeholder="Select a feature view"
                    isLoading={featureViewsQuery.isLoading}
                    compressed
                  />
                </EuiFormRow>
              </EuiFlexItem>
              <EuiFlexItem>
                <EuiFormRow
                  label="Features (optional)"
                  helpText="Leave empty to include all features from the view."
                >
                  <EuiComboBox
                    options={featureOptions}
                    selectedOptions={selectedFeatureOptions}
                    onChange={(selected) =>
                      updateProjection(
                        index,
                        "featureNames",
                        selected.map((option) => option.value || option.label),
                      )
                    }
                    placeholder={
                      projection.featureViewName
                        ? "All features"
                        : "Select a feature view first"
                    }
                    isDisabled={!projection.featureViewName}
                    compressed
                  />
                </EuiFormRow>
              </EuiFlexItem>
              <EuiFlexItem grow={false}>
                <EuiFormRow hasEmptyLabelSpace>
                  <EuiButtonIcon
                    iconType="trash"
                    color="danger"
                    aria-label="Remove feature view"
                    onClick={() => removeProjection(index)}
                    disabled={formData.projections.length <= 1}
                  />
                </EuiFormRow>
              </EuiFlexItem>
            </EuiFlexGroup>
          </React.Fragment>
        );
      })}

      <EuiSpacer size="m" />

      <TagsEditor
        tags={formData.tags}
        onChange={(tags) => updateField("tags", tags)}
        error={errors.tags}
      />
    </FormModal>
  );
};

export default FeatureServiceFormModal;
export type { FeatureServiceFormData, FeatureViewProjectionEntry };
