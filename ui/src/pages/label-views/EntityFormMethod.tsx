import React, { useState, useContext, useEffect, useMemo } from "react";
import { useParams } from "react-router-dom";
import {
  EuiCallOut,
  EuiSpacer,
  EuiFlexGroup,
  EuiFlexItem,
  EuiButton,
  EuiPanel,
  EuiTitle,
  EuiText,
  EuiLoadingSpinner,
  EuiFormRow,
  EuiFieldText,
  EuiFieldNumber,
  EuiSelect,
  EuiTextArea,
  EuiButtonGroup,
  EuiBadge,
  EuiIcon,
  EuiEmptyPrompt,
  EuiForm,
} from "@elastic/eui";
import RegistryPathContext from "../../contexts/RegistryPathContext";
import useLoadLabelView from "./useLoadLabelView";
import { AnnotationConfig } from "./useAnnotationConfig";

interface EntityFormMethodProps {
  annotationConfig: AnnotationConfig;
}

const EntityFormMethod = ({ annotationConfig }: EntityFormMethodProps) => {
  const { labelViewName } = useParams();
  const registryUrl = useContext(RegistryPathContext);
  const { data } = useLoadLabelView(labelViewName || "");

  const spec = data?.object?.spec || data?.spec || {};
  const entities: string[] = spec.entities || [];
  const labelFields: { name: string; valueType?: string }[] =
    spec.features || [];

  const fieldRoles = annotationConfig.field_roles;
  const labelValues = annotationConfig.label_values;
  const labelWidgets = annotationConfig.label_widgets;
  const labelerField = annotationConfig.labeler_field || "labeler";

  const [entityValues, setEntityValues] = useState<Record<string, string>>({});
  const [fieldInputs, setFieldInputs] = useState<Record<string, string>>({});
  const [isSaving, setIsSaving] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [pushSuccess, setPushSuccess] = useState<string | null>(null);
  const [labelCount, setLabelCount] = useState(0);

  const editableFields = useMemo(
    () =>
      labelFields.filter(
        (f) => f.name !== labelerField && f.name !== "event_timestamp",
      ),
    [labelFields, labelerField],
  );

  useEffect(() => {
    const defaults: Record<string, string> = {};
    editableFields.forEach((f) => {
      const vals = labelValues[f.name];
      if (vals && vals.length > 0) {
        defaults[f.name] = "";
      } else {
        defaults[f.name] = "";
      }
    });
    setFieldInputs(defaults);
  }, [editableFields, labelValues]);

  const resetForm = () => {
    const defaults: Record<string, string> = {};
    editableFields.forEach((f) => {
      defaults[f.name] = "";
    });
    setFieldInputs(defaults);
    setError(null);
    setPushSuccess(null);
  };

  const isFormValid = useMemo(() => {
    const hasEntity = entities.every(
      (e) => entityValues[e] && entityValues[e].trim() !== "",
    );
    const hasAtLeastOneLabel = editableFields.some(
      (f) =>
        fieldRoles[f.name] === "label" &&
        fieldInputs[f.name] &&
        fieldInputs[f.name].trim() !== "",
    );
    return hasEntity && hasAtLeastOneLabel;
  }, [entities, entityValues, editableFields, fieldInputs, fieldRoles]);

  const submitLabel = async () => {
    if (!isFormValid) return;

    setIsSaving(true);
    setError(null);
    setPushSuccess(null);

    try {
      const baseUrl = registryUrl?.replace(/\/$/, "") || "/api/v1";
      const pushSourceName =
        annotationConfig.push_source_name ||
        spec.source?.pushSourceName ||
        spec.source?.name ||
        `${labelViewName}_push_source`;

      const pushRow: Record<string, any> = {};
      entities.forEach((e) => {
        pushRow[e] = entityValues[e];
      });
      editableFields.forEach((f) => {
        if (fieldInputs[f.name] && fieldInputs[f.name].trim() !== "") {
          const widget = labelWidgets[f.name];
          if (widget === "number" || widget === "binary") {
            pushRow[f.name] = Number(fieldInputs[f.name]);
          } else {
            pushRow[f.name] = fieldInputs[f.name];
          }
        }
      });
      pushRow[labelerField] = fieldInputs[labelerField] || "human_reviewer";
      pushRow["event_timestamp"] = new Date().toISOString();

      const columnar: Record<string, any[]> = {};
      for (const key of Object.keys(pushRow)) {
        columnar[key] = [pushRow[key]];
      }

      const response = await fetch(`${baseUrl}/push`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          push_source_name: pushSourceName,
          df: columnar,
          to: "online_and_offline",
        }),
      });

      if (response.ok) {
        setLabelCount((c) => c + 1);
        setPushSuccess(
          `Label pushed for ${entities.map((e) => `${e}=${entityValues[e]}`).join(", ")}`,
        );
        resetForm();
      } else {
        const errData = await response.json().catch(() => null);
        setError(errData?.detail || `Push failed (${response.status})`);
      }
    } catch (e: any) {
      setError(e.message || "Network error");
    } finally {
      setIsSaving(false);
    }
  };

  const renderFieldInput = (field: { name: string; valueType?: string }) => {
    const widget = labelWidgets[field.name];
    const values = labelValues[field.name];
    const role = fieldRoles[field.name];
    const currentValue = fieldInputs[field.name] || "";

    if (widget === "binary" && values && values.length === 2) {
      const options = values.map((v) => ({
        id: v,
        label: v === "1" ? "Yes" : v === "0" ? "No" : v,
      }));
      return (
        <EuiButtonGroup
          legend={`Select ${field.name}`}
          options={options}
          idSelected={currentValue}
          onChange={(id) =>
            setFieldInputs((prev) => ({ ...prev, [field.name]: id }))
          }
          buttonSize="m"
        />
      );
    }

    if (
      widget === "enum" ||
      (values && values.length > 0 && values.length <= 30)
    ) {
      return (
        <EuiSelect
          options={[
            { value: "", text: `— select ${field.name} —` },
            ...(values || []).map((v) => ({ value: v, text: v })),
          ]}
          value={currentValue}
          onChange={(e) =>
            setFieldInputs((prev) => ({
              ...prev,
              [field.name]: e.target.value,
            }))
          }
        />
      );
    }

    if (widget === "number") {
      return (
        <EuiFieldNumber
          value={currentValue}
          onChange={(e) =>
            setFieldInputs((prev) => ({
              ...prev,
              [field.name]: e.target.value,
            }))
          }
          placeholder={`Enter ${field.name}`}
        />
      );
    }

    if (widget === "text" || role === "metadata") {
      return (
        <EuiTextArea
          value={currentValue}
          onChange={(e) =>
            setFieldInputs((prev) => ({
              ...prev,
              [field.name]: e.target.value,
            }))
          }
          placeholder={`Enter ${field.name}`}
          rows={2}
          compressed
        />
      );
    }

    return (
      <EuiFieldText
        value={currentValue}
        onChange={(e) =>
          setFieldInputs((prev) => ({
            ...prev,
            [field.name]: e.target.value,
          }))
        }
        placeholder={`Enter ${field.name}`}
      />
    );
  };

  if (!labelViewName) {
    return (
      <EuiEmptyPrompt
        iconType="editorStrike"
        title={<h3>No label view selected</h3>}
      />
    );
  }

  return (
    <React.Fragment>
      <EuiCallOut
        title="Entity annotation form"
        color="primary"
        iconType="documentEdit"
        size="s"
      >
        <p>
          Fill in the entity identifier and label values below. Each submission
          pushes one label record to <strong>{labelViewName}</strong>.
        </p>
      </EuiCallOut>

      {error && (
        <>
          <EuiSpacer size="m" />
          <EuiCallOut title="Error" color="danger" iconType="alert">
            <p>{error}</p>
          </EuiCallOut>
        </>
      )}

      {pushSuccess && (
        <>
          <EuiSpacer size="m" />
          <EuiCallOut title={pushSuccess} color="success" iconType="check" />
        </>
      )}

      <EuiSpacer size="l" />

      <EuiFlexGroup gutterSize="l">
        <EuiFlexItem grow={3}>
          <EuiPanel hasBorder paddingSize="l">
            <EuiForm component="form">
              <EuiTitle size="xs">
                <h4>
                  <EuiIcon type="user" /> Entity
                </h4>
              </EuiTitle>
              <EuiSpacer size="m" />

              {entities.map((entity) => (
                <EuiFormRow key={entity} label={entity} fullWidth>
                  <EuiFieldText
                    fullWidth
                    value={entityValues[entity] || ""}
                    onChange={(e) =>
                      setEntityValues((prev) => ({
                        ...prev,
                        [entity]: e.target.value,
                      }))
                    }
                    placeholder={`Enter ${entity} value`}
                  />
                </EuiFormRow>
              ))}

              <EuiSpacer size="l" />
              <EuiTitle size="xs">
                <h4>
                  <EuiIcon type="tag" /> Labels
                </h4>
              </EuiTitle>
              <EuiSpacer size="m" />

              {editableFields
                .filter((f) => fieldRoles[f.name] === "label")
                .map((field) => (
                  <EuiFormRow
                    key={field.name}
                    label={field.name}
                    fullWidth
                    helpText={
                      field.valueType ? `Type: ${field.valueType}` : undefined
                    }
                  >
                    {renderFieldInput(field)}
                  </EuiFormRow>
                ))}

              {editableFields.filter(
                (f) =>
                  fieldRoles[f.name] !== "label" &&
                  fieldRoles[f.name] !== undefined,
              ).length > 0 && (
                <>
                  <EuiSpacer size="l" />
                  <EuiTitle size="xs">
                    <h4>
                      <EuiIcon type="annotation" /> Additional Fields
                    </h4>
                  </EuiTitle>
                  <EuiSpacer size="m" />
                  {editableFields
                    .filter(
                      (f) =>
                        fieldRoles[f.name] !== "label" &&
                        fieldRoles[f.name] !== undefined,
                    )
                    .map((field) => (
                      <EuiFormRow key={field.name} label={field.name} fullWidth>
                        {renderFieldInput(field)}
                      </EuiFormRow>
                    ))}
                </>
              )}

              {editableFields.filter((f) => !fieldRoles[f.name]).length > 0 && (
                <>
                  <EuiSpacer size="l" />
                  {editableFields
                    .filter((f) => !fieldRoles[f.name])
                    .map((field) => (
                      <EuiFormRow key={field.name} label={field.name} fullWidth>
                        {renderFieldInput(field)}
                      </EuiFormRow>
                    ))}
                </>
              )}

              <EuiSpacer size="l" />

              <EuiFormRow label="Labeler identity" fullWidth>
                <EuiFieldText
                  fullWidth
                  value={fieldInputs[labelerField] || ""}
                  onChange={(e) =>
                    setFieldInputs((prev) => ({
                      ...prev,
                      [labelerField]: e.target.value,
                    }))
                  }
                  placeholder="your_name or reviewer_id"
                />
              </EuiFormRow>

              <EuiSpacer size="l" />

              <EuiFlexGroup gutterSize="m">
                <EuiFlexItem grow={false}>
                  <EuiButton
                    fill
                    color="success"
                    onClick={submitLabel}
                    isLoading={isSaving}
                    disabled={!isFormValid}
                    iconType="push"
                  >
                    Submit Label
                  </EuiButton>
                </EuiFlexItem>
                <EuiFlexItem grow={false}>
                  <EuiButton color="text" onClick={resetForm}>
                    Clear
                  </EuiButton>
                </EuiFlexItem>
              </EuiFlexGroup>
            </EuiForm>
          </EuiPanel>
        </EuiFlexItem>

        <EuiFlexItem grow={1}>
          <EuiPanel hasBorder paddingSize="m" color="subdued">
            <EuiTitle size="xxs">
              <h4>Session</h4>
            </EuiTitle>
            <EuiSpacer size="s" />
            <EuiText size="s">
              <p>
                <strong>Labels submitted:</strong>{" "}
                <EuiBadge color="success">{labelCount}</EuiBadge>
              </p>
              <p>
                <strong>Conflict policy:</strong>{" "}
                <EuiBadge color="hollow">
                  {spec.conflictPolicy || "LAST_WRITE_WINS"}
                </EuiBadge>
              </p>
              <p>
                <strong>Labeler field:</strong> {labelerField}
              </p>
            </EuiText>
            <EuiSpacer size="m" />
            <EuiTitle size="xxs">
              <h4>Schema</h4>
            </EuiTitle>
            <EuiSpacer size="s" />
            <EuiText size="xs">
              {entities.map((e) => (
                <p key={e}>
                  <EuiBadge color="hollow">entity</EuiBadge> {e}
                </p>
              ))}
              {editableFields.map((f) => (
                <p key={f.name}>
                  <EuiBadge
                    color={
                      fieldRoles[f.name] === "label" ? "primary" : "default"
                    }
                  >
                    {fieldRoles[f.name] || "field"}
                  </EuiBadge>{" "}
                  {f.name}
                </p>
              ))}
            </EuiText>
          </EuiPanel>
        </EuiFlexItem>
      </EuiFlexGroup>
    </React.Fragment>
  );
};

export default EntityFormMethod;
