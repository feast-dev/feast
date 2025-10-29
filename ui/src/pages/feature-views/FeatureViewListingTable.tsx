import React from "react";
import {
  EuiBasicTable,
  EuiBadge,
  EuiTableFieldDataColumnType,
} from "@elastic/eui";
import EuiCustomLink from "../../components/EuiCustomLink";
import { genericFVType } from "../../parsers/mergedFVTypes";
import { EuiTableComputedColumnType } from "@elastic/eui/src/components/basic_table";
import { useParams } from "react-router-dom";

interface FeatureViewListingTableProps {
  tagKeysSet: Set<string>;
  featureViews: genericFVType[];
}

type genericFVTypeColumn =
  | EuiTableFieldDataColumnType<genericFVType>
  | EuiTableComputedColumnType<genericFVType>;

const FeatureViewListingTable = ({
  tagKeysSet,
  featureViews,
}: FeatureViewListingTableProps) => {
  const { projectName } = useParams();

  const columns: genericFVTypeColumn[] = [
    {
      name: "Name",
      field: "name",
      sortable: true,
      render: (name: string, item: genericFVType) => {
        // For "All Projects" view, link to the specific project
        const itemProject = item.object?.spec?.project || projectName;
        return (
          <EuiCustomLink to={`/p/${itemProject}/feature-view/${name}`}>
            {name}{" "}
            {(item.type === "ondemand" && <EuiBadge>ondemand</EuiBadge>) ||
              (item.type === "stream" && <EuiBadge>stream</EuiBadge>)}
          </EuiCustomLink>
        );
      },
    },
    {
      name: "# of Features",
      field: "features",
      sortable: true,
      render: (features: unknown[]) => {
        return features.length;
      },
    },
  ];

  // Add Project column when viewing all projects
  if (projectName === "all") {
    columns.splice(1, 0, {
      name: "Project",
      render: (item: genericFVType) => {
        return <span>{item.object?.spec?.project || "Unknown"}</span>;
      },
    });
  }

  // Add columns if they come up in search
  tagKeysSet.forEach((key) => {
    columns.push({
      name: key,
      render: (item: genericFVType) => {
        let tag = <span>n/a</span>;

        if (item.type === "regular") {
          const value = item?.object?.spec!.tags
            ? item.object.spec.tags[key]
            : undefined;

          if (value) {
            tag = <span>{value}</span>;
          }
        }

        return tag;
      },
    });
  });

  const getRowProps = (item: genericFVType) => {
    return {
      "data-test-subj": `row-${item.name}`,
    };
  };

  return (
    <EuiBasicTable
      columns={columns}
      items={featureViews}
      rowProps={getRowProps}
    />
  );
};

export default FeatureViewListingTable;
