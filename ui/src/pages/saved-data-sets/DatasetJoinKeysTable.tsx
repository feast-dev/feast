import { EuiBasicTable } from "@elastic/eui";
import React from "react";

interface DatasetJoinKey {
  name: string;
}

interface DatasetJoinKeysTableProps {
  joinKeys: DatasetJoinKey[];
}

const DatasetJoinKeysTable = ({ joinKeys }: DatasetJoinKeysTableProps) => {
  const columns = [
    {
      name: "Name",
      field: "name",
    },
  ];

  return <EuiBasicTable columns={columns} items={joinKeys} />;
};

export default DatasetJoinKeysTable;
