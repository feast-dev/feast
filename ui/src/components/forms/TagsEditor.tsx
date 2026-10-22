import React from "react";
import {
  EuiFlexGroup,
  EuiFlexItem,
  EuiFieldText,
  EuiButtonEmpty,
  EuiButtonIcon,
  EuiText,
  EuiHorizontalRule,
  EuiSpacer,
  EuiCallOut,
} from "@elastic/eui";

interface TagEntry {
  key: string;
  value: string;
}

interface TagsEditorProps {
  tags: TagEntry[];
  onChange: (tags: TagEntry[]) => void;
  error?: string;
}

const TagsEditor: React.FC<TagsEditorProps> = ({ tags, onChange, error }) => {
  const addTag = () => {
    onChange([...tags, { key: "", value: "" }]);
  };

  const removeTag = (index: number) => {
    onChange(tags.filter((_, i) => i !== index));
  };

  const updateTag = (index: number, field: "key" | "value", val: string) => {
    const updated = [...tags];
    updated[index] = { ...updated[index], [field]: val };
    onChange(updated);
  };

  return (
    <>
      <EuiHorizontalRule margin="s" />
      <EuiFlexGroup alignItems="center" justifyContent="spaceBetween">
        <EuiFlexItem grow={false}>
          <EuiText size="s">
            <h4>Labels</h4>
          </EuiText>
        </EuiFlexItem>
        <EuiFlexItem grow={false}>
          <EuiButtonEmpty
            size="s"
            iconType="plus"
            onClick={addTag}
            flush="right"
          >
            Add label
          </EuiButtonEmpty>
        </EuiFlexItem>
      </EuiFlexGroup>

      {error && (
        <>
          <EuiSpacer size="s" />
          <EuiCallOut title={error} color="danger" size="s" />
        </>
      )}

      {tags.map((tag, index) => (
        <EuiFlexGroup
          key={index}
          gutterSize="s"
          alignItems="center"
          style={{ marginTop: 4 }}
        >
          <EuiFlexItem>
            <EuiFieldText
              placeholder="Key"
              value={tag.key}
              onChange={(e) => updateTag(index, "key", e.target.value)}
              compressed
            />
          </EuiFlexItem>
          <EuiFlexItem>
            <EuiFieldText
              placeholder="Value"
              value={tag.value}
              onChange={(e) => updateTag(index, "value", e.target.value)}
              compressed
            />
          </EuiFlexItem>
          <EuiFlexItem grow={false}>
            <EuiButtonIcon
              iconType="trash"
              color="danger"
              aria-label="Remove label"
              onClick={() => removeTag(index)}
            />
          </EuiFlexItem>
        </EuiFlexGroup>
      ))}

      {tags.length === 0 && (
        <EuiText size="xs" color="subdued">
          No labels added yet.
        </EuiText>
      )}
    </>
  );
};

export default TagsEditor;
export type { TagEntry };
