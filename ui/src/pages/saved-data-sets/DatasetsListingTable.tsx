import React from "react";
import { EuiBasicTable } from "@elastic/eui";
import EuiCustomLink from "../../components/EuiCustomLink";
import { useParams } from "react-router";
import { FeastSavedDatasetType } from "../../parsers/feastSavedDataset";

interface DatasetsListingTableProps {
  datasets: FeastSavedDatasetType[];
}

const DatasetsListingTable = ({ datasets }: DatasetsListingTableProps) => {
  const { projectName } = useParams();

  const columns = [
    {
      name: "Name",
      field: "spec.name",
      sortable: true,
      render: (name: string) => {
        return (
          <EuiCustomLink
            href={`/p/${projectName}/data-set/${name}`}
            to={`/p/${projectName}/data-set/${name}`}
          >
            {name}
          </EuiCustomLink>
        );
      },
    },
    {
      name: "Source Feature Service",
      field: "spec.featureService",
    },
    {
      name: "Created",
      render: (item: FeastSavedDatasetType) => {
        return item.meta.createdTimestamp.toLocaleDateString("en-CA");
      },
    },
  ];

  const getRowProps = (item: FeastSavedDatasetType) => {
    return {
      "data-test-subj": `row-${item.spec.name}`,
    };
  };

  return (
    <EuiBasicTable columns={columns} items={datasets} rowProps={getRowProps} />
  );
};

export default DatasetsListingTable;
