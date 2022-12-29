import React from "react";
import { EuiBasicTable } from "@elastic/eui";
import EuiCustomLink from "../../components/EuiCustomLink";
import { FeastEntityType } from "../../parsers/feastEntities";
import useFeatureViewEdgesByEntity from "./useFeatureViewEdgesByEntity";
import { useParams } from "react-router-dom";

interface EntitiesListingTableProps {
  entities: FeastEntityType[];
}

const EntitiesListingTable = ({ entities }: EntitiesListingTableProps) => {
  const { isSuccess, data } = useFeatureViewEdgesByEntity();
  const { projectName } = useParams();

  const columns = [
    {
      name: "Name",
      field: "spec.name",
      sortable: true,
      render: (name: string) => {
        return (
          <EuiCustomLink
            href={`/p/${projectName}/entity/${name}`}
            to={`/p/${projectName}/entity/${name}`}
          >
            {name}
          </EuiCustomLink>
        );
      },
    },
    {
      name: "Type",
      field: "spec.valueType",
      sortable: true,
      render: (valueType: string) => {
        return valueType;
      },
    },
    {
      name: "# of FVs",
      render: (item: FeastEntityType) => {
        if (isSuccess && data) {
          return data[item.spec.name] ? data[item.spec.name].length : "0";
        } else {
          return ".";
        }
      },
    },
  ];

  const getRowProps = (item: FeastEntityType) => {
    return {
      "data-test-subj": `row-${item.spec.name}`,
    };
  };

  return (
    <EuiBasicTable columns={columns} items={entities} rowProps={getRowProps} />
  );
};

export default EntitiesListingTable;
