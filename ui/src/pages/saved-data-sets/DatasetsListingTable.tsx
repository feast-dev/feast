import React from "react";
import { EuiBasicTable } from "@elastic/eui";
import EuiCustomLink from "../../components/EuiCustomLink";
import { useParams } from "react-router-dom";
import { feast } from "../../protos";
import { toDate } from "../../utils/timestamp";

interface DatasetsListingTableProps {
  datasets: feast.core.ISavedDataset[];
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
            to={`${process.env.PUBLIC_URL || ""}/p/${projectName}/data-set/${name}`}
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
      render: (item: feast.core.ISavedDataset) => {
        return toDate(item?.meta?.createdTimestamp!).toLocaleString("en-CA")!;
      },
    },
  ];

  const getRowProps = (item: feast.core.ISavedDataset) => {
    return {
      "data-test-subj": `row-${item.spec?.name}`,
    };
  };

  return (
    <EuiBasicTable columns={columns} items={datasets} rowProps={getRowProps} />
  );
};

export default DatasetsListingTable;
