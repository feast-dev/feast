import React from "react";
import { EuiBasicTable } from "@elastic/eui";
import EuiCustomLink from "../../components/EuiCustomLink";
import { FeastDatasourceType } from "../../parsers/feastDatasources";
import { useParams } from "react-router-dom";

interface DatasourcesListingTableProps {
  dataSources: FeastDatasourceType[];
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
    },
  ];

  const getRowProps = (item: FeastDatasourceType) => {
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
