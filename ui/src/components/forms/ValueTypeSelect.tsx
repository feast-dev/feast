import React from "react";
import { EuiFormRow, EuiSelect } from "@elastic/eui";
import { feast } from "../../protos";

const VALUE_TYPE_OPTIONS = [
  { value: String(feast.types.ValueType.Enum.STRING), text: "STRING" },
  { value: String(feast.types.ValueType.Enum.INT32), text: "INT32" },
  { value: String(feast.types.ValueType.Enum.INT64), text: "INT64" },
  { value: String(feast.types.ValueType.Enum.FLOAT), text: "FLOAT" },
  { value: String(feast.types.ValueType.Enum.DOUBLE), text: "DOUBLE" },
  { value: String(feast.types.ValueType.Enum.BOOL), text: "BOOL" },
  { value: String(feast.types.ValueType.Enum.BYTES), text: "BYTES" },
  {
    value: String(feast.types.ValueType.Enum.UNIX_TIMESTAMP),
    text: "UNIX_TIMESTAMP",
  },
];

interface ValueTypeSelectProps {
  value: string;
  onChange: (value: string) => void;
  label?: string;
  helpText?: string;
  compressed?: boolean;
}

const ValueTypeSelect: React.FC<ValueTypeSelectProps> = ({
  value,
  onChange,
  label = "Value Type",
  helpText,
  compressed = false,
}) => {
  return (
    <EuiFormRow label={label} helpText={helpText}>
      <EuiSelect
        options={VALUE_TYPE_OPTIONS}
        value={value}
        onChange={(e) => onChange(e.target.value)}
        compressed={compressed}
      />
    </EuiFormRow>
  );
};

export default ValueTypeSelect;
export { VALUE_TYPE_OPTIONS };
