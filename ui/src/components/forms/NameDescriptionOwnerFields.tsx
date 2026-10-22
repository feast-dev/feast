import React from "react";
import { EuiFormRow, EuiFieldText, EuiTextArea } from "@elastic/eui";

interface NameDescriptionOwnerFieldsProps {
  name: string;
  description: string;
  owner?: string;
  onChangeName: (value: string) => void;
  onChangeDescription: (value: string) => void;
  onChangeOwner?: (value: string) => void;
  nameDisabled?: boolean;
  nameError?: string;
  nameHelpText?: string;
  namePlaceholder?: string;
  descriptionPlaceholder?: string;
}

const NameDescriptionOwnerFields: React.FC<NameDescriptionOwnerFieldsProps> = ({
  name,
  description,
  owner,
  onChangeName,
  onChangeDescription,
  onChangeOwner,
  nameDisabled = false,
  nameError,
  nameHelpText,
  namePlaceholder = "e.g. my_resource",
  descriptionPlaceholder = "Describe this resource...",
}) => {
  return (
    <>
      <EuiFormRow
        label="Name"
        isInvalid={!!nameError}
        error={nameError}
        helpText={nameDisabled ? undefined : nameHelpText}
      >
        <EuiFieldText
          value={name}
          onChange={(e) => onChangeName(e.target.value)}
          isInvalid={!!nameError}
          disabled={nameDisabled}
          placeholder={namePlaceholder}
        />
      </EuiFormRow>

      <EuiFormRow label="Description" helpText="Optional description.">
        <EuiTextArea
          value={description}
          onChange={(e) => onChangeDescription(e.target.value)}
          placeholder={descriptionPlaceholder}
          rows={2}
        />
      </EuiFormRow>

      {onChangeOwner !== undefined && (
        <EuiFormRow label="Owner" helpText="Optional owner identifier.">
          <EuiFieldText
            value={owner || ""}
            onChange={(e) => onChangeOwner(e.target.value)}
            placeholder="e.g. team-ml-platform"
          />
        </EuiFormRow>
      )}
    </>
  );
};

export default NameDescriptionOwnerFields;
