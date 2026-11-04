import React from "react";
import {
  EuiFlexGroup,
  EuiFlexItem,
  EuiFieldText,
  EuiSelect,
  EuiButtonEmpty,
  EuiButtonIcon,
  EuiText,
  EuiHorizontalRule,
  EuiSpacer,
  EuiCallOut,
} from "@elastic/eui";
import { VALUE_TYPE_OPTIONS } from "./ValueTypeSelect";
import { feast } from "../../protos";

interface FeatureFieldEntry {
  name: string;
  valueType: string;
  description: string;
}

interface FeatureFieldEditorProps {
  features: FeatureFieldEntry[];
  onChange: (features: FeatureFieldEntry[]) => void;
  error?: string;
}

const EMPTY_FEATURE: FeatureFieldEntry = {
  name: "",
  valueType: String(feast.types.ValueType.Enum.INT64),
  description: "",
};

const FeatureFieldEditor: React.FC<FeatureFieldEditorProps> = ({
  features,
  onChange,
  error,
}) => {
  const addFeature = () => {
    onChange([...features, { ...EMPTY_FEATURE }]);
  };

  const removeFeature = (index: number) => {
    onChange(features.filter((_, i) => i !== index));
  };

  const updateFeature = (
    index: number,
    field: keyof FeatureFieldEntry,
    val: string,
  ) => {
    const updated = [...features];
    updated[index] = { ...updated[index], [field]: val };
    onChange(updated);
  };

  return (
    <>
      <EuiHorizontalRule margin="s" />
      <EuiFlexGroup alignItems="center" justifyContent="spaceBetween">
        <EuiFlexItem grow={false}>
          <EuiText size="s">
            <h4>Features</h4>
          </EuiText>
        </EuiFlexItem>
        <EuiFlexItem grow={false}>
          <EuiButtonEmpty
            size="s"
            iconType="plus"
            onClick={addFeature}
            flush="right"
          >
            Add feature
          </EuiButtonEmpty>
        </EuiFlexItem>
      </EuiFlexGroup>

      {error && (
        <>
          <EuiSpacer size="s" />
          <EuiCallOut title={error} color="danger" size="s" />
        </>
      )}

      {features.length > 0 && (
        <EuiFlexGroup gutterSize="s" style={{ marginTop: 4 }}>
          <EuiFlexItem grow={3}>
            <EuiText size="xs" color="subdued">
              Name
            </EuiText>
          </EuiFlexItem>
          <EuiFlexItem grow={2}>
            <EuiText size="xs" color="subdued">
              Type
            </EuiText>
          </EuiFlexItem>
          <EuiFlexItem grow={3}>
            <EuiText size="xs" color="subdued">
              Description
            </EuiText>
          </EuiFlexItem>
          <EuiFlexItem grow={false} style={{ width: 32 }} />
        </EuiFlexGroup>
      )}

      {features.map((feature, index) => (
        <EuiFlexGroup
          key={index}
          gutterSize="s"
          alignItems="center"
          style={{ marginTop: 4 }}
        >
          <EuiFlexItem grow={3}>
            <EuiFieldText
              placeholder="feature_name"
              value={feature.name}
              onChange={(e) => updateFeature(index, "name", e.target.value)}
              compressed
            />
          </EuiFlexItem>
          <EuiFlexItem grow={2}>
            <EuiSelect
              options={VALUE_TYPE_OPTIONS}
              value={feature.valueType}
              onChange={(e) =>
                updateFeature(index, "valueType", e.target.value)
              }
              compressed
            />
          </EuiFlexItem>
          <EuiFlexItem grow={3}>
            <EuiFieldText
              placeholder="Optional description"
              value={feature.description}
              onChange={(e) =>
                updateFeature(index, "description", e.target.value)
              }
              compressed
            />
          </EuiFlexItem>
          <EuiFlexItem grow={false}>
            <EuiButtonIcon
              iconType="trash"
              color="danger"
              aria-label="Remove feature"
              onClick={() => removeFeature(index)}
            />
          </EuiFlexItem>
        </EuiFlexGroup>
      ))}

      {features.length === 0 && (
        <EuiText size="xs" color="subdued">
          No features added yet. Click "Add feature" above.
        </EuiText>
      )}
    </>
  );
};

export default FeatureFieldEditor;
export type { FeatureFieldEntry };
