import React from "react";
import {
  EuiBasicTable,
  EuiTableComputedColumnType,
  EuiTableFieldDataColumnType,
} from "@elastic/eui";
import EuiCustomLink from "../../components/EuiCustomLink";
import {
  FeastFeatureInServiceType,
  FeastFeatureServiceType,
} from "../../parsers/feastFeatureServices";
import { useParams } from "react-router";

interface FeatureServiceListingTableProps {
  tagKeysSet: Set<string>;
  featureServices: FeastFeatureServiceType[];
}

type FeatureServiceTypeColumn =
  | EuiTableFieldDataColumnType<FeastFeatureServiceType>
  | EuiTableComputedColumnType<FeastFeatureServiceType>;

const FeatureServiceListingTable = ({
  tagKeysSet,
  featureServices,
}: FeatureServiceListingTableProps) => {
  const { projectName } = useParams();

  const columns: FeatureServiceTypeColumn[] = [
    {
      name: "Name",
      field: "spec.name",
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
    {
      name: "# of Features",
      field: "spec.features",
      render: (featureViews: FeastFeatureInServiceType[]) => {
        var numFeatures = 0;
        featureViews.forEach((featureView) => {
          numFeatures += featureView.featureColumns.length;
        });
        return numFeatures;
      },
    },
    {
      name: "Created at",
      field: "meta.createdTimestamp",
      render: (date: Date) => {
        return date.toLocaleDateString("en-CA");
      },
    },
  ];

  tagKeysSet.forEach((key) => {
    columns.push({
      name: key,
      render: (item: FeastFeatureServiceType) => {
        let tag = <span>n/a</span>;

        const value = item.spec.tags ? item.spec.tags[key] : undefined;

        if (value) {
          tag = <span>{value}</span>;
        }

        return tag;
      },
    });
  });

  const getRowProps = (item: FeastFeatureServiceType) => {
    return {
      "data-test-subj": `row-${item.spec.name}`,
    };
  };

  return (
    <EuiBasicTable
      columns={columns}
      items={featureServices}
      rowProps={getRowProps}
    />
  );
};

export default FeatureServiceListingTable;
