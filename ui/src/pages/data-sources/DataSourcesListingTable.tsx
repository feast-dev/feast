import React from "react";
import { EuiBasicTable } from "@elastic/eui";
import EuiCustomLink from "../../components/EuiCustomLink";
import { useParams } from "react-router-dom";
import { feast } from "../../protos";

interface DatasourcesListingTableProps {
  dataSources: feast.core.IDataSource[];
}

const DatasourcesListingTable = ({
  dataSources,
}: DatasourcesListingTableProps) => {
  const { projectName } = useParams();

  const columns = [
    {
      name: "Name",
      field: "name",
      sortable: true,
      render: (name: string) => {
        return (
          <EuiCustomLink
            href={`/p/${projectName}/data-source/${name}`}
            to={`/p/${projectName}/data-source/${name}`}
          >
            {name}
          </EuiCustomLink>
        );
      },
    },
    {
      name: "Type",
      field: "type",
      sortable: true,
      render: (valueType: feast.types.ValueType.Enum) => {
        return feast.types.ValueType.Enum[valueType];
      },
    },
  ];

  const getRowProps = (item: feast.core.IDataSource) => {
    return {
      "data-test-subj": `row-${item.name}`,
    };
  };

  return (
    <EuiBasicTable
      columns={columns}
      items={dataSources}
      rowProps={getRowProps}
    />
  );
};

export default DatasourcesListingTable;
