import React from "react";
import { EuiBasicTable } from "@elastic/eui";
import EuiCustomLink from "../../components/EuiCustomLink";
import { useParams } from "react-router-dom";

interface ConsumingFeatureServicesListInterace {
  fsNames: string[];
}

const ConsumingFeatureServicesList = ({
  fsNames,
}: ConsumingFeatureServicesListInterace) => {
  const { projectName } = useParams();

  const columns = [
    {
      name: "Name",
      field: "",
      render: (name: string) => {
        return (
          <EuiCustomLink
            href={`/p/${projectName}/feature-service/${name}`}
            to={`/p/${projectName}/feature-service/${name}`}
          >
            {name}
          </EuiCustomLink>
        );
      },
    },
  ];

  const getRowProps = (item: string) => {
    return {
      "data-test-subj": `row-${item}`,
    };
  };

  return (
    <EuiBasicTable columns={columns} items={fsNames} rowProps={getRowProps} />
  );
};

export default ConsumingFeatureServicesList;
