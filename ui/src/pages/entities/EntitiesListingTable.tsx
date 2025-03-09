import React from "react";
import { EuiBasicTable } from "@elastic/eui";
import EuiCustomLink from "../../components/EuiCustomLink";
import useFeatureViewEdgesByEntity from "./useFeatureViewEdgesByEntity";
import { useParams } from "react-router-dom";
import { feast } from "../../protos";

interface EntitiesListingTableProps {
  entities: feast.core.IEntity[];
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
          <EuiCustomLink to={`/p/${projectName}/entity/${name}`}>
            {name}
          </EuiCustomLink>
        );
      },
    },
    {
      name: "Type",
      field: "spec.valueType",
      sortable: true,
      render: (valueType: feast.types.ValueType.Enum) => {
        return feast.types.ValueType.Enum[valueType];
      },
    },
    {
      name: "# of FVs",
      render: (item: feast.core.IEntity) => {
        if (isSuccess && data) {
          return data[item?.spec?.name!] ? data[item?.spec?.name!].length : "0";
        } else {
          return ".";
        }
      },
    },
  ];

  const getRowProps = (item: feast.core.IEntity) => {
    return {
      "data-test-subj": `row-${item?.spec?.name}`,
    };
  };

  return (
    <EuiBasicTable columns={columns} items={entities} rowProps={getRowProps} />
  );
};

export default EntitiesListingTable;
