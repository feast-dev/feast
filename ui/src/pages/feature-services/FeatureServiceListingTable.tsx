import React from "react";
import {
  EuiBasicTable,
  EuiTableComputedColumnType,
  EuiTableFieldDataColumnType,
} from "@elastic/eui";
import EuiCustomLink from "../../components/EuiCustomLink";
import { useParams } from "react-router-dom";
import { feast } from "../../protos";
import { toDate } from "../../utils/timestamp";

interface FeatureServiceListingTableProps {
  tagKeysSet: Set<string>;
  featureServices: feast.core.IFeatureService[];
}

type FeatureServiceTypeColumn =
  | EuiTableFieldDataColumnType<feast.core.IFeatureService>
  | EuiTableComputedColumnType<feast.core.IFeatureService>;

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
          <EuiCustomLink to={`/p/${projectName}/feature-service/${name}`}>
            {name}
          </EuiCustomLink>
        );
      },
    },
    {
      name: "# of Features",
      field: "spec.features",
      render: (featureViews: feast.core.IFeatureViewProjection[]) => {
        var numFeatures = 0;
        featureViews.forEach((featureView) => {
          numFeatures += featureView.featureColumns!.length;
        });
        return numFeatures;
      },
    },
    {
      name: "Last updated",
      field: "meta.lastUpdatedTimestamp",
      render: (date: any) => {
        return date ? toDate(date).toLocaleDateString("en-CA") : "n/a";
      },
    },
  ];

  tagKeysSet.forEach((key) => {
    columns.push({
      name: key,
      render: (item: feast.core.IFeatureService) => {
        let tag = <span>n/a</span>;

        const value = item?.spec?.tags ? item.spec.tags[key] : undefined;

        if (value) {
          tag = <span>{value}</span>;
        }

        return tag;
      },
    });
  });

  const getRowProps = (item: feast.core.IFeatureService) => {
    return {
      "data-test-subj": `row-${item?.spec?.name}`,
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
