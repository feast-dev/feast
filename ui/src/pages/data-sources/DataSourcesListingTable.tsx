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
      render: (name: string, item: feast.core.IDataSource) => {
        // For "All Projects" view, link to the specific project
        const itemProject = item?.project || projectName;
        return (
          <EuiCustomLink to={`/p/${itemProject}/data-source/${name}`}>
            {name}
          </EuiCustomLink>
        );
      },
    },
    {
      name: "Type",
      field: "type",
      sortable: true,
      render: (valueType: feast.core.DataSource.SourceType) => {
        return feast.core.DataSource.SourceType[valueType];
      },
    },
  ];

  // Add Project column when viewing all projects
  if (projectName === "all") {
    columns.splice(1, 0, {
      name: "Project",
      field: "project",
      sortable: true,
      render: (project: string) => {
        return <span>{project || "Unknown"}</span>;
      },
    });
  }

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
