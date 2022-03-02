import React from "react";
import { EuiBasicTable } from "@elastic/eui";
import { FEAST_FEATURE_VALUE_TYPES } from "../../parsers/types";

interface RequestDataSourceSchemaField {
  fieldName: string;
  valueType: FEAST_FEATURE_VALUE_TYPES;
}

interface RequestDataSourceSchema {
  fields: RequestDataSourceSchemaField[];
}

const RequestDataSourceSchemaTable = ({ fields }: RequestDataSourceSchema) => {
  console.log(fields);
  const columns = [
    {
      name: "Field",
      field: "fieldName",
    },
    {
      name: "Value Type",
      field: "valueType",
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
