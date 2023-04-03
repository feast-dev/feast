import React from "react";
import { EuiBasicTable } from "@elastic/eui";
import { feast } from "../../protos";

interface RequestDataSourceSchemaField {
  fieldName: string;
  valueType: feast.types.ValueType.Enum;
}

interface RequestDataSourceSchema {
  fields: RequestDataSourceSchemaField[];
}

const RequestDataSourceSchemaTable = ({ fields }: RequestDataSourceSchema) => {
  const columns = [
    {
      name: "Field",
      field: "fieldName",
    },
    {
      name: "Value Type",
      field: "valueType",
      render: (valueType: feast.types.ValueType.Enum) => {
        return feast.types.ValueType.Enum[valueType];
      },
    },
  ];

  const getRowProps = (item: RequestDataSourceSchemaField) => {
    return {
      "data-test-subj": `row-${item.fieldName}`,
    };
  };

  return (
    <EuiBasicTable columns={columns} items={fields} rowProps={getRowProps} />
  );
};

export default RequestDataSourceSchemaTable;
